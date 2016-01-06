/*
 * @(#)Master.java $version 2016. 1. 4.
 *
 * Copyright 2007 NHN Corp. All rights Reserved. 
 * NHN PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.asuraiv.coordination;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import com.asuraiv.coordination.enums.TaskStatus;
import com.asuraiv.coordination.enums.WorkerStatus;
import com.asuraiv.coordination.util.ZooKeeperUtils;

/**
 * @author Jupyo Hong
 */
public class Master extends Thread implements Watcher, AsyncCallback.ChildrenCallback {

	private static final Logger LOG = LoggerFactory.getLogger(Master.class);

	private ZooKeeperUtils zkUtils;

	/**
	 * @param string
	 * @throws IOException 
	 */
	public Master(String hostPort) throws IOException {
		zkUtils = new ZooKeeperUtils(new ZooKeeper(hostPort, 15000, this));
	}

	/**
	 * @param event
	 * @see org.apache.zookeeper.Watcher#process(org.apache.zookeeper.WatchedEvent)
	 */
	public void process(WatchedEvent event) {
		// 다시 Watcher를 등록한다
		zkUtils.asyncGetChildren("/tasks", true, /* AsyncCallback.ChildrenCallback */this, null);
	}

	/**
	 * @param rc
	 * @param path
	 * @param ctx
	 * @param children
	 * @see org.apache.zookeeper.AsyncCallback.ChildrenCallback#processResult(int, java.lang.String, java.lang.Object, java.util.List)
	 */
	public void processResult(int rc, String path, Object ctx, List<String> children) {

		if (CollectionUtils.isEmpty(children)) {
			return;
		}		

		for (String task : children) {

			String taskStatus = zkUtils.getData("/tasks/" + task);
			if (!TaskStatus.WAITING.name().equals(taskStatus)) {
				continue;
			}
			
			// 가용한 Worker를 가져온다.
			String availableWorker = getAvailableWorkers();

			LOG.info("[MASTER] {} 에게 {} 작업을 할당 합니다.", availableWorker, task);
			zkUtils.asyncCreate("/assign/" + availableWorker, task.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, null, null);

			// 다시 Watcher를 등록한다
			zkUtils.asyncGetChildren("/tasks", true, /* AsyncCallback.ChildrenCallback */this, null);
		}
	}

	/**
	 * 가용한 Worker(Idle 상태인)를 찾는다.
	 * 
	 * @return
	 */
	private String getAvailableWorkers() {
		
		while (true) {
			
			List<String> workers = zkUtils.getChildren("/workers", false, null);
			for (String worker : workers) {

				String workerStatus = zkUtils.getData("/workers/" + worker);
				
				if (WorkerStatus.IDLE.name().equals(workerStatus)) {
					// 가용한 worker를 찾으면 메소드 종료
					return worker;
				}
			}

			LOG.info("[MASTER] 가용한 Worker가 없습니다. 다시 찾습니다...");

			try {
				Thread.currentThread().sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void run() {
		
		LOG.info("[MASTER] 마스터를 구동합니다...");

		zkUtils.asyncGetChildren("/tasks", true, /* AsyncCallback.ChildrenCallback */this, null);

		try {
			synchronized (this) {
				while (true) {
					wait();
				}
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
