package com.mycompany.aquariam;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.AsyncCallback.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import static org.apache.zookeeper.KeeperException.Code.NONODE;
import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

/**
 *
 */
public class Client implements Watcher {

	public static final Logger LOG = Logger.getLogger(Client.class);
	private static Random random = new Random();

	private ZooKeeper zk;
	private String hostPort;

	public Client(String hostPort) {
		this.hostPort = hostPort;
	}

	public void startZK() throws IOException {
		zk = new ZooKeeper(hostPort, 15000, this);
	}

	public String queueCommand(String command) throws Exception {
		while (true) {
			String name = null;
			try {
				name = zk.create("/tasks/task-",
						command.getBytes(), OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
				return name;
			} catch (KeeperException.ConnectionLossException e) {

			} catch (KeeperException.NodeExistsException e) {
				throw new Exception(name + " already appears to be running");
			}
		}
	}

	void submitTask(String task, TaskObject taskCtx) {
		StringCallback createTaskCallback = new StringCallback() {
			public void processResult(int rc, String path, Object ctx, String name) {
				switch (KeeperException.Code.get(rc)) {
					case CONNECTIONLOSS:
						/*
						 * Handling connection loss for a sequential node is a bit
						 * delicate. Executing the ZooKeeper create command again
						 * might lead to duplicate tasks. For now, let's assume
						 * that it is ok to create a duplicate task.
						 */
						submitTask(((TaskObject) ctx).getTask(), (TaskObject) ctx);

						break;
					case OK:
						LOG.info("My created task name: " + name);
						((TaskObject) ctx).setTaskName(name);
						watchStatus(name.replace("/tasks/", "/status/"), ctx);

						break;
					default:
						LOG.error("Something went wrong" + KeeperException.create(KeeperException.Code.get(rc), path));
				}
			}
		};
		taskCtx.setTask(task);
		zk.create("/tasks/task-",
				task.getBytes(),
				ZooDefs.Ids.OPEN_ACL_UNSAFE,
				CreateMode.PERSISTENT_SEQUENTIAL,
				createTaskCallback,
				taskCtx);
	}

	protected ConcurrentHashMap<String, Object> ctxMap = new ConcurrentHashMap<String, Object>();

	void watchStatus(String path, Object ctx) {
		VoidCallback taskDeleteCallback = new VoidCallback() {
			public void processResult(int rc, String path, Object ctx) {
				switch (KeeperException.Code.get(rc)) {
					case CONNECTIONLOSS:
						zk.delete(path, -1, this, null);
						break;
					case OK:
						LOG.info("Successfully deleted " + path);
						break;
					default:
						LOG.error("Something went wrong here, " +
								KeeperException.create(KeeperException.Code.get(rc), path));
				}
			}
		};

		DataCallback getDataCallback = new DataCallback() {
			public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
				switch (KeeperException.Code.get(rc)) {
					case CONNECTIONLOSS:
						/*
						 * Try again.
						 */
						zk.getData(path, false, this, ctxMap.get(path));
						return;
					case OK:
						/*
						 *  Print result
						 */
						String taskResult = new String(data);
						LOG.info("Task " + path + ", " + taskResult);

						/*
						 *  Setting the status of the task
						 */
						assert (ctx != null);
						((TaskObject) ctx).setStatus(taskResult.contains("done"));

						/*
						 *  Delete status znode
						 */
						//zk.delete("/tasks/" + path.replace("/status/", ""), -1, taskDeleteCallback, null);
						zk.delete(path, -1, taskDeleteCallback, null);
						ctxMap.remove(path);
						break;
					case NONODE:
						LOG.warn("Status node is gone!");
						return;
					default:
						LOG.error("Something went wrong here, " +
								KeeperException.create(KeeperException.Code.get(rc), path));
				}
			}
		};
		Watcher statusWatcher = new Watcher() {
			public void process(WatchedEvent e) {
				if (e.getType() == Event.EventType.NodeCreated) {
					assert e.getPath().contains("/status/task-");
					assert ctxMap.containsKey(e.getPath());

					zk.getData(e.getPath(),
							false,
							getDataCallback,
							ctxMap.get(e.getPath()));
				}
			}
		};
		StatCallback existsCallback = new StatCallback() {
			public void processResult(int rc, String path, Object ctx, Stat stat) {
				switch (KeeperException.Code.get(rc)) {
					case CONNECTIONLOSS:
						watchStatus(path, ctx);

						break;
					case OK:
						if (stat != null) {
							zk.getData(path, false, getDataCallback, ctx);
							LOG.info("Status node is there: " + path);
						}

						break;
					case NONODE:
						break;
					default:
						LOG.error("Something went wrong when " +
								"checking if the status node exists: " +
								KeeperException.create(KeeperException.Code.get(rc), path));

						break;
				}
			}
		};
		ctxMap.put(path, ctx);
		zk.exists(path,
				statusWatcher,
				existsCallback,
				ctx);
	}

	@Override
	public void process(WatchedEvent event) {
		System.out.println(event);
	}

	static class TaskObject {
		private String task;
		private String taskName;
		private boolean done = false;
		private boolean succesful = false;
		private CountDownLatch latch = new CountDownLatch(1);

		String getTask() {
			return task;
		}

		void setTask(String task) {
			this.task = task;
		}

		void setTaskName(String name) {
			this.taskName = name;
		}

		String getTaskName() {
			return taskName;
		}

		void setStatus(boolean status) {
			succesful = status;
			done = true;
			latch.countDown();
		}

		void waitUntilDone() {
			try {
				latch.await();
			} catch (InterruptedException e) {
				LOG.warn("InterruptedException while waiting for task to get done");
			}
		}

		synchronized boolean isDone() {
			return done;
		}

		synchronized boolean isSuccesful() {
			return succesful;
		}

	}

	public static void main(String[] args) throws Exception {
		Client c1 = new Client(args[0]);
		c1.startZK();
		String name = c1.queueCommand(args[1]);
		System.out.println("Created " + name);
	}
}
