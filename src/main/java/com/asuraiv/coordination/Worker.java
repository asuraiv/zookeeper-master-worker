/*
 * @(#)Worker.java $version 2016. 1. 4.
 *
 * Copyright 2007 NHN Corp. All rights Reserved. 
 * NHN PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.asuraiv.coordination;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asuraiv.coordination.enums.TaskStatus;
import com.asuraiv.coordination.enums.WorkerStatus;
import com.asuraiv.coordination.util.ZooKeeperUtils;

/**
 * @author Jupyo Hong
 */
public class Worker implements Runnable, Watcher, AsyncCallback.ChildrenCallback {

	private static final Logger LOG = LoggerFactory.getLogger(Worker.class);

	private ZooKeeperUtils zkUtils;
	
	private String workerName;
	
	private StringCallback createZNodeCallback = new StringCallback() {

		public void processResult(int rc, String path, Object ctx, String name) {

			switch (Code.get(rc)) {
				case CONNECTIONLOSS:
					createWorkerZNode((byte[])ctx);
					break;
				case OK:
					workerName = name.substring(name.lastIndexOf("/") + 1);
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
	 * @param hostPort
	 * @throws IOException 
	 */
	public Worker(String hostPort) throws IOException {
		zkUtils = new ZooKeeperUtils(new ZooKeeper(hostPort, 15000, this));
	}
	
	/**
	 * 
	 * @param event
	 * @see org.apache.zookeeper.Watcher#process(org.apache.zookeeper.WatchedEvent)
	 */
	public void process(WatchedEvent event) {
		zkUtils.asyncGetChildren("/assign", true, /* AsyncCallback.ChildrenCallback */this, null);
	}

	/**
	 * @param rc
	 * @param path
	 * @param ctx
	 * @param children
	 * @see org.apache.zookeeper.AsyncCallback.ChildrenCallback#processResult(int, java.lang.String, java.lang.Object, java.util.List)
	 */
	public void processResult(int rc, String path, Object ctx, List<String> childrenOfAssignNode) {
		LOG.info("[WORKER] {} 가 일을 찾고 있습니다...", workerName);
		for (String child : childrenOfAssignNode) {
			// 자기의 일을 찾는다
			if (child.equals(workerName)) {
				doTasksProcess();
			}
		}
	}

	/**
	 * 일 처리!
	 */
	private void doTasksProcess() {
		
		String task = zkUtils.getData("/assign/" + workerName);
		// task 상태를 RUNNING 으로 변경
		zkUtils.setData("/tasks/" + task, TaskStatus.RUNNING.name());
		// worker 자신의 상태도 Busy로 바꾼다
		zkUtils.setData("/workers/" + workerName, WorkerStatus.BUSY.toString());

		for (int i = 0; i < 10; i++) {
			LOG.info("[WORKER][{}] '{}' 일을 하는 중입니다...", workerName, task);
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {					
			}
		}
		LOG.info("[WORKER][{}] '{}' 일을 끝냈습니다!", workerName, task);
		
		// task 를 끝냈으므로 상태를 DONE으로 변경
		zkUtils.setData("/tasks/" + task, TaskStatus.DONE.name());
		// 다시 worker 자신의 상태를 Idle로 바꾼다
		zkUtils.setData("/workers/" + workerName, WorkerStatus.IDLE.name());
		// assign 노드에 할당된 task를 지운다
		zkUtils.delete("/assign/" + workerName);
	}
	
	/**
	 * @see java.lang.Runnable#run()
	 */
	public void run() {

		// worker 노드 생성
		String status = WorkerStatus.IDLE.name();
		createWorkerZNode(status.getBytes());

		zkUtils.asyncGetChildren("/assign", true, /* AsyncCallback.ChildrenCallback */this, null);

		try {
			synchronized (this) {
				while (true) {
					wait();
				}
			}
		} catch (InterruptedException e) {
		}
	}

	/**
	 * @param data
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	private void createWorkerZNode(byte[] data) {
		zkUtils.asyncCreate("/workers/worker-", data, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL, createZNodeCallback, null);
	}
}
