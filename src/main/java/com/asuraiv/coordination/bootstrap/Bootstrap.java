/*
 * @(#)Bootstrap.java $version 2016. 1. 4.
 *
 * Copyright 2007 NHN Corp. All rights Reserved. 
 * NHN PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.asuraiv.coordination.bootstrap;

import java.io.IOException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import com.asuraiv.coordination.Master;
import com.asuraiv.coordination.Worker;

/**
 * @author Jupyo Hong
 */
public class Bootstrap {

	private static ZooKeeper zk;

	private static StringCallback createZNodeCallback = new StringCallback() {

		public void processResult(int rc, String path, Object ctx, String name) {
			switch (Code.get(rc)) {
				case CONNECTIONLOSS:
					createZNode(path, (byte[])ctx);
					break;
				case OK:
					// Log 추가
					break;
				case NODEEXISTS:
					// Log 추가
					break;
				default:
					// Log 추가
					break;
			}
		}
	};

	/**
	 * Master-Worker Coordination 프로그램을 실행한다.
	 * 
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		
		if(args.length < 1) {
			System.out.println("#### Usage: java -jar zk-master-worker.jar [host:port] [worker count]");
			return ;
		}

		String hostPort = args[0];

		zk = new ZooKeeper(hostPort, 15000, null);
		
		// 기본 ZNode 생성
		initZNodes();

		Master master = new Master(hostPort);
		master.start();
		
		int workerCount = Integer.parseInt(args[1]);
		
		ThreadPoolExecutor executor = new ThreadPoolExecutor(10, 20, 60, TimeUnit.SECONDS, new SynchronousQueue<Runnable>());
		
		for (int i = 0; i < workerCount; i++) {
			executor.execute(new Worker(hostPort));
		}
	}
	
	/**
	 * 
	 * 초기 기본 ZNode 들을 생성한다.
	 */
	private static void initZNodes() {
		createZNode("/workers", new byte[0]);
		createZNode("/assign", new byte[0]);
		createZNode("/tasks", new byte[0]);
	}

	/**
	 * @param string
	 * @param bs
	 */
	private static void createZNode(String path, byte[] data) {
		zk.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, createZNodeCallback, data);
	}
}
