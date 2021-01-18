package com.mycompany.aquariam;

import com.mycompany.aquariam.helper.ChildrenCache;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.AsyncCallback.*;
import org.apache.zookeeper.KeeperException.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

/**
 *
 */
public class Master implements Watcher {

	public static final Logger LOG = Logger.getLogger(Master.class);

	enum MasterStates {RUNNING, ELECTED, NOTELECTED}

	;

	private ZooKeeper zk;
	private String hostPort;
	public String serverId;
	private static Random random = new Random();
	private MasterStates state;

	public Master(String hostPort) {
		this.hostPort = hostPort;
		this.serverId = Integer.toHexString(random.nextInt());
		this.state = MasterStates.RUNNING;
	}

	public void bootstrap() {
		createParent("/workers", new byte[0]);
		createParent("/assign", new byte[0]);
		createParent("/tasks", new byte[0]);
		createParent("/status", new byte[0]);

	}

	void createParent(String path, byte[] data) {
		zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, createParentCallback, data);
	}

	StringCallback createParentCallback = new StringCallback() {
		@Override
		public void processResult(int rc, String path, Object ctx, String name) {
			switch (Code.get(rc)) {
				case CONNECTIONLOSS:
					createParent(path, (byte[]) ctx);
					break;
				case OK:
					LOG.info("Parent created");
					break;
				case NODEEXISTS:
					LOG.warn("Parent already registered " + path);
					break;
				default:
					LOG.error("Something went wrong ", KeeperException.create(Code.get(rc), path));
			}
		}
	};

	public void runForMaster() throws InterruptedException {
		while (true) {

			try {
				zk.create("/master",
						serverId.getBytes(),
						OPEN_ACL_UNSAFE,
						CreateMode.EPHEMERAL);
			} catch (ConnectionLossException e) {

			} catch (KeeperException e) {
				this.state = MasterStates.NOTELECTED;
				break;
			}
			if (checkMaster()) break;
		}
	}

	boolean checkMaster() throws InterruptedException {
		while (true) {
			try {
				Stat stat = new Stat();
				byte data[] = zk.getData("/master", false, stat);
				this.state = new String(data).equals(serverId) ? MasterStates.ELECTED : MasterStates.NOTELECTED;
				return true;
			} catch (ConnectionLossException e) {

			} catch (KeeperException e) {
				return false;
			}
		}
	}

	public void runForMasterAsync() {
		StringCallback masterCreateCallback = new StringCallback() {
			@Override
			public void processResult(int rc, String path, Object ctx, String name) {
				switch (Code.get(rc)) {
					case CONNECTIONLOSS:
						checkMasterAsync();
						return;
					case OK:
						state = MasterStates.ELECTED;
						takeLeadership();
						break;
					case NODEEXISTS:
						state = MasterStates.NOTELECTED;
						masterExists();
					default:
						state = MasterStates.NOTELECTED;
						LOG.error("Something wrong " + KeeperException.create(Code.get(rc), path));
				}
			}
		};
		zk.create("/master", serverId.getBytes(), OPEN_ACL_UNSAFE,
				CreateMode.EPHEMERAL, masterCreateCallback, null);
	}

	private void takeLeadership() {
		System.out.println("I am the leader now " + serverId);
	}

	private void masterExists() {
		Watcher masterExistsWatch = new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				if (event.getType() == Event.EventType.NodeDeleted) {
					assert "/master".equals(event.getPath());
					runForMasterAsync();
				}
			}
		};

		StatCallback masterExistsCallback = new StatCallback() {
			@Override
			public void processResult(int rc, String path, Object ctx, Stat stat) {
				switch (Code.get(rc)) {
					case CONNECTIONLOSS:
						masterExists();
						break;
					case OK:
						if (stat == null) {
							state = MasterStates.RUNNING;
							runForMasterAsync();
						}
						break;
					default:
						checkMasterAsync();
						break;
				}
			}
		};
		zk.exists("/master",
				masterExistsWatch,
				masterExistsCallback,
				null);
	}

	public void checkMasterAsync() {
		DataCallback masterCheckCallback = new DataCallback() {
			@Override
			public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
				switch (Code.get(rc)) {
					case CONNECTIONLOSS:
						checkMasterAsync();
						return;
					case NONODE:
						runForMasterAsync();
						return;
					case OK:
						//note that we don't do anything if master is taken
						return;
				}
			}
		};
		zk.getData("/master", false, masterCheckCallback, null);
	}


	public void getWorkers() {
		Watcher workersChangeWatcher = new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				if (event.getType() == Event.EventType.NodeChildrenChanged) {
					assert "/workers".equals(event.getPath());
					getWorkers();
				}
			}
		};

		Children2Callback workersGetChhildrenCallback = new Children2Callback() {
			@Override
			public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
				switch (Code.get(rc)) {
					case CONNECTIONLOSS:
						getWorkers();
						break;
					case OK:
						LOG.info("Successfully got a list of " + children.size() + " workers");
						reassignAndSet(children);
						break;
					default:
						LOG.error("get children failed: ", KeeperException.create(Code.get(rc), path));

				}
			}
		};

		zk.getChildren("/workers",
				workersChangeWatcher,
				workersGetChhildrenCallback,
				null);
	}

	private ChildrenCache workerCache;

	private void reassignAndSet(List<String> children) {
		List<String> toProcess;
		if (workerCache == null) {
			workerCache = new ChildrenCache(children);
			toProcess = null;
		} else {
			LOG.info("Resetting workers");
			toProcess = workerCache.removedAndSet(children);
		}
		if (toProcess != null) {
			for (String worker : toProcess) {
				getAbsentWorkerTasks(worker);
			}
		}
	}

	private void getAbsentWorkerTasks(String worker) {
		ChildrenCallback workerAssignmentCallback = new ChildrenCallback() {
			@Override
			public void processResult(int rc, String path, Object ctx, List<String> children) {
				switch (Code.get(rc)) {
					case CONNECTIONLOSS:
						getAbsentWorkerTasks(path);
						break;
					case OK:
						LOG.info("Successfully found worker replacement");
						for (String t : children) {
							getDataReassign(path + "/" + t, t);
						}
						break;
					default:
						LOG.error("getChildren failed", KeeperException.create(Code.get(rc), path));
				}
			}
		};
		zk.getChildren("/assign" + worker,
				false,
				workerAssignmentCallback,
				null);
	}

	private void getDataReassign(String path, String task) {
		DataCallback getDataReassignCallback = new DataCallback() {
			public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
				switch (Code.get(rc)) {
					case CONNECTIONLOSS:
						getDataReassign(path, (String) ctx);

						break;
					case OK:
						recreateTask(new RecreateTaskCtx(path, (String) ctx, data));

						break;
					default:
						LOG.error("Something went wrong when getting data ",
								KeeperException.create(Code.get(rc)));
				}
			}
		};
		zk.getData(path,
				false,
				getDataReassignCallback,
				task);
	}

	void recreateTask(RecreateTaskCtx ctx) {
		StringCallback recreateTaskCallback = new StringCallback() {
			public void processResult(int rc, String path, Object ctx, String name) {
				switch (Code.get(rc)) {
					case CONNECTIONLOSS:
						recreateTask((RecreateTaskCtx) ctx);

						break;
					case OK:
						deleteAssignment(((RecreateTaskCtx) ctx).path);

						break;
					case NODEEXISTS:
						LOG.info("Node exists already, but if it hasn't been deleted, " +
								"then it will eventually, so we keep trying: " + path);
						recreateTask((RecreateTaskCtx) ctx);

						break;
					default:
						LOG.error("Something wwnt wrong when recreating task",
								KeeperException.create(Code.get(rc)));
				}
			}
		};
		zk.create("/tasks/" + ctx.task,
				ctx.data,
				ZooDefs.Ids.OPEN_ACL_UNSAFE,
				CreateMode.PERSISTENT,
				recreateTaskCallback,
				ctx);
	}

	class RecreateTaskCtx {
		String path;
		String task;
		byte[] data;

		RecreateTaskCtx(String path, String task, byte[] data) {
			this.path = path;
			this.task = task;
			this.data = data;
		}
	}

	void deleteAssignment(String path) {
		zk.delete(path, -1, taskDeletionCallback, null);
	}

	VoidCallback taskDeletionCallback = new VoidCallback() {
		public void processResult(int rc, String path, Object rtx) {
			switch (Code.get(rc)) {
				case CONNECTIONLOSS:
					deleteAssignment(path);
					break;
				case OK:
					LOG.info("Task correctly deleted: " + path);
					break;
				default:
					LOG.error("Failed to delete task data" +
							KeeperException.create(Code.get(rc), path));
			}
		}
	};

	public void getTasks() {
		Watcher tasksChangeWatcher = new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				if (event.getType() == Event.EventType.NodeChildrenChanged) {
					getTasks();
				}
			}
		};
		ChildrenCallback tasksGetChildrenCallback = new ChildrenCallback() {
			@Override
			public void processResult(int rc, String path, Object ctx, List<String> children) {
				switch (Code.get(rc)) {
					case CONNECTIONLOSS:
						getTasks();
						break;
					case OK:
						if (children != null) {
							assignTasks(children);
						}
						break;
					default:
						LOG.error("get children failed ", KeeperException.create(Code.get(rc), path));
				}
			}
		};
		zk.getChildren("/tasks",
				tasksChangeWatcher,
				tasksGetChildrenCallback,
				null);
	}

	void assignTasks(List<String> tasks) {
		for (String task : tasks) {
			getTaskData(task);
		}
	}

	void getTaskData(String task) {
		DataCallback taskDataCallback = new DataCallback() {
			public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
				switch (Code.get(rc)) {
					case CONNECTIONLOSS:
						getTaskData((String) ctx);

						break;
					case OK:
						/*
						 * Choose worker at random.
						 */
						List<String> list = workerCache.getList();
						String designatedWorker = list.get(random.nextInt(list.size()));

						/*
						 * Assign task to randomly chosen worker.
						 */
						String assignmentPath = "/assign/" +
								designatedWorker +
								"/" +
								(String) ctx;
						LOG.info("Assignment path: " + assignmentPath);
						createAssignment(assignmentPath, data);

						break;
					default:
						LOG.error("Error when trying to get task data.",
								KeeperException.create(Code.get(rc), path));
				}
			}
		};
		zk.getData("/tasks/" + task,
				false,
				taskDataCallback,
				task);
	}

	void createAssignment(String path, byte[] data) {
		StringCallback assignTaskCallback = new StringCallback() {
			public void processResult(int rc, String path, Object ctx, String name) {
				switch (Code.get(rc)) {
					case CONNECTIONLOSS:
						createAssignment(path, (byte[]) ctx);

						break;
					case OK:
						LOG.info("Task assigned correctly: " + name);
						deleteTask(name.substring(name.lastIndexOf("/") + 1));

						break;
					case NODEEXISTS:
						LOG.warn("Task already assigned");

						break;
					default:
						LOG.error("Error when trying to assign task.",
								KeeperException.create(Code.get(rc), path));
				}
			}
		};
		zk.create(path,
				data,
				ZooDefs.Ids.OPEN_ACL_UNSAFE,
				CreateMode.PERSISTENT,
				assignTaskCallback,
				data);
	}

	void deleteTask(String name) {

		VoidCallback taskDeleteCallback = new VoidCallback() {
			public void processResult(int rc, String path, Object ctx) {
				switch (Code.get(rc)) {
					case CONNECTIONLOSS:
						deleteTask(path);

						break;
					case OK:
						LOG.info("Successfully deleted " + path);

						break;
					case NONODE:
						LOG.info("Task has been deleted already");

						break;
					default:
						LOG.error("Something went wrong here, " +
								KeeperException.create(Code.get(rc), path));
				}
			}
		};
		zk.delete("/tasks/" + name, -1, taskDeleteCallback, null);
	}


	public void startZK() throws IOException {
		zk = new ZooKeeper(hostPort, 15000, this);
	}

	public void stopZK() throws InterruptedException {
		zk.close();
	}

	public void process(WatchedEvent event) {
		System.out.println(event);
	}

	public MasterStates getState() {
		return this.state;
	}

	public static void main(String[] args) throws Exception {
		Master m1 = new Master(args[0]);
		m1.startZK();
		m1.runForMasterAsync();
		Thread.sleep(5000);
		if (m1.getState() == MasterStates.ELECTED) {
			System.out.println("I'm the leader " + m1.serverId);
		} else {
			System.out.println("Someone else is the leader");
		}

		m1.stopZK();
	}
}
