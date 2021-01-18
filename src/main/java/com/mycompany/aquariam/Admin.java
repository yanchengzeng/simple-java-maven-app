package com.mycompany.aquariam;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Date;
import java.util.Random;

/**
 *
 */
public class Admin implements Watcher {
	public static final Logger LOG = Logger.getLogger(Admin.class);
	private static Random random = new Random();

	private ZooKeeper zk;
	private String hostPort;

	public Admin(String hostPort) {
		this.hostPort = hostPort;
	}

	public void startZK() throws IOException {
		zk = new ZooKeeper(hostPort, 15000, this);
	}

	public void listState() throws KeeperException, InterruptedException {
		try {
			Stat stat = new Stat();
			byte[] masterData = zk.getData("/master", false, stat);
			Date startDate = new Date(stat.getCtime());
			System.out.println("Master: " + new String(masterData) + " since " + startDate);

		} catch (KeeperException.NoNodeException e) {
			System.out.println("No master");
		}

		System.out.println("Workers:");
		for (String w : zk.getChildren("/workers", false)) {
			byte[] data = zk.getData("/workers/" + w, false, null);
			String state = new String(data);
			System.out.println("\t" + w + ": " + state);
		}

		System.out.println("Tasks:");
		for (String t : zk.getChildren("/tasks", false)) {
			System.out.println("\t " + t);
		}
	}


	@Override
	public void process(WatchedEvent event) {
		System.out.println(event);
	}

	public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
		Admin a1 = new Admin(args[0]);
		a1.startZK();
		a1.listState();
	}
}
