package com.mycompany.aquariam;

import com.mycompany.aquariam.helper.ChildrenCache;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class Worker implements Watcher {

	public static final Logger LOG = Logger.getLogger(Worker.class);
	private static Random random = new Random();

	private ZooKeeper zk;
	private String hostPort;
	public String serverId = Integer.toHexString(random.nextInt());
	public String name = "worker-" + serverId;
	private String status;
	private ThreadPoolExecutor executor;

	public Worker(String hostPort) {
		this.hostPort = hostPort;
		this.status = "Idle";
		this.executor = new ThreadPoolExecutor(1, 1,
				1000L,
				TimeUnit.MILLISECONDS,
				new ArrayBlockingQueue<Runnable>(200));
	}

	public void startZK() throws IOException {
		zk = new ZooKeeper(hostPort, 15000, this);
	}

	public void process(WatchedEvent event) {
		LOG.info(event.toString() + ", " + hostPort);
	}

	public void register() {
		StringCallback createWorkerCallback = new StringCallback() {
			@Override
			public void processResult(int rc, String path, Object ctx, String name) {
				switch (KeeperException.Code.get(rc)) {
					case CONNECTIONLOSS:
						register();
						break;
					case OK:
						LOG.info("Registered successfully: " + serverId);
						break;
					case NODEEXISTS:
						LOG.warn("Already registered: " + serverId);
						break;
					default:
						LOG.error("Something went wrong: " + KeeperException.create(KeeperException.Code.get(rc), path));
				}
			}
		};
		zk.create("/workers/" + this.name,
				this.status.getBytes(),
				ZooDefs.Ids.OPEN_ACL_UNSAFE,
				CreateMode.EPHEMERAL,
				createWorkerCallback, null);
	}

	protected ChildrenCache assignedTasksCache = new ChildrenCache();

	public void getTasks() {
		Watcher newTaskWatcher = new Watcher() {
			public void process(WatchedEvent e) {
				if (e.getType() == Event.EventType.NodeChildrenChanged) {
					assert new String("/assign/worker-" + serverId).equals(e.getPath());

					getTasks();
				}
			}
		};

		StringCallback taskStatusCreateCallback = new StringCallback() {
			public void processResult(int rc, String path, Object ctx, String name) {
				switch (KeeperException.Code.get(rc)) {
					case CONNECTIONLOSS:
						zk.create(path + "/status", "done".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
								this, null);
						break;
					case OK:
						LOG.info("Created status znode correctly: " + name);
						break;
					case NODEEXISTS:
						LOG.warn("Node exists: " + path);
						break;
					default:
						LOG.error("Failed to create task data: ", KeeperException.create(KeeperException.Code.get(rc), path));
				}

			}
		};
		VoidCallback taskVoidCallback = new VoidCallback() {
			public void processResult(int rc, String path, Object rtx) {
				switch (KeeperException.Code.get(rc)) {
					case CONNECTIONLOSS:
						break;
					case OK:
						LOG.info("Task correctly deleted: " + path);
						break;
					default:
						LOG.error("Failed to delete task data" + KeeperException.create(KeeperException.Code.get(rc), path));
				}
			}
		};
		DataCallback taskDataCallback = new DataCallback() {
			public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
				switch (KeeperException.Code.get(rc)) {
					case CONNECTIONLOSS:
						zk.getData(path, false, this, null);
						break;
					case OK:
						/*
						 *  Executing a task in this example is simply printing out
						 *  some string representing the task.
						 */
						executor.execute(new Runnable() {
							byte[] data;
							Object ctx;

							/*
							 * Initializes the variables this anonymous class needs
							 */
							public Runnable init(byte[] data, Object ctx) {
								this.data = data;
								this.ctx = ctx;

								return this;
							}

							public void run() {
								LOG.info("Executing your task: " + new String(data));
								zk.create("/status/" + (String) ctx, "done".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
										CreateMode.PERSISTENT, taskStatusCreateCallback, null);
								zk.delete("/assign/worker-" + serverId + "/" + (String) ctx,
										-1, taskVoidCallback, null);
							}
						}.init(data, ctx));

						break;
					default:
						LOG.error("Failed to get task data: ", KeeperException.create(KeeperException.Code.get(rc), path));
				}
			}
		};
		ChildrenCallback tasksGetChildrenCallback = new ChildrenCallback() {
			public void processResult(int rc, String path, Object ctx, List<String> children) {
				switch (KeeperException.Code.get(rc)) {
					case CONNECTIONLOSS:
						getTasks();
						break;
					case OK:
						if (children != null) {
							executor.execute(new Runnable() {
								List<String> children;
								DataCallback cb;

								/*
								 * Initializes input of anonymous class
								 */
								public Runnable init(List<String> children, DataCallback cb) {
									this.children = children;
									this.cb = cb;

									return this;
								}

								public void run() {
									if (children == null) {
										return;
									}

									LOG.info("Looping into tasks");
									setStatus("Working");
									for (String task : children) {
										LOG.trace("New task: " + task);
										zk.getData("/assign/worker-" + serverId + "/" + task,
												false,
												cb,
												task);
									}
								}
							}.init(assignedTasksCache.addedAndSet(children), taskDataCallback));
						}
						break;
					default:
						System.out.println("getChildren failed: " + KeeperException.create(KeeperException.Code.get(rc), path));
				}
			}
		};
	}

	synchronized private void updateStatus(String status) {
		AsyncCallback.StatCallback statusUpdateCallback = new AsyncCallback.StatCallback() {
			@Override
			public void processResult(int rc, String path, Object ctx, Stat stat) {
				switch (KeeperException.Code.get(rc)) {
					case CONNECTIONLOSS:
						updateStatus((String) ctx);
						return;
				}
			}
		};
		if (status == this.status) {
			zk.setData("workers/" + this.name, status.getBytes(), -1, statusUpdateCallback, status);
		}
	}

	public void setStatus(String status) {
		this.status = status;
		updateStatus(status);
	}

	public static void main(String[] args) throws Exception {
		Worker w1 = new Worker(args[0]);
		w1.startZK();
		w1.register();
		Thread.sleep(15000);
	}
}
