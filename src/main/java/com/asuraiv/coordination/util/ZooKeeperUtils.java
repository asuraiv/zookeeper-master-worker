/*
 * @(#)ZooKeeperUtils.java $version 2016. 1. 5.
 *
 * Copyright 2007 NHN Corp. All rights Reserved. 
 * NHN PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.asuraiv.coordination.util;

import java.util.List;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

/**
 * @author Jupyo Hong
 */
public class ZooKeeperUtils {
	
	private ZooKeeper zk;
	
	public ZooKeeperUtils(ZooKeeper zk) {
		this.zk = zk;
	}
	
	public List<String> getChildren(String path) {
		return getChildren(path, false, null);
	}

	/**
	 * @param path
	 * @param b
	 * @param object
	 * @return
	 */
	public List<String> getChildren(String path, boolean watch, Stat stat) {
		try {
			return zk.getChildren(path, watch, stat);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * @param string
	 * @return
	 */
	public String getData(String path) {
		try {
			return new String(zk.getData(path, false, null));
		} catch (KeeperException e) {
			if(e.getMessage().contains("NoNode")) {
				System.err.println(e.getMessage());
			} else {
				e.printStackTrace();
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * @param string
	 * @param b
	 * @param master
	 * @param ctx
	 */
	public void asyncGetChildren(String path, boolean watch, AsyncCallback.ChildrenCallback callback, Object ctx) {
		zk.getChildren(path, watch, callback, ctx);
	}
	
	public void setData(String path, String data) {
		try {
			zk.setData(path, data.getBytes(), -1);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public String create(String path, byte data[], List<ACL> acl, CreateMode createMode) {
		try {
			return zk.create(path, data, acl, createMode);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public void asyncCreate(String path, byte data[], List<ACL> acl, CreateMode createMode, StringCallback cb, Object ctx) {
		zk.create(path, data, acl, createMode, cb, ctx);
	}
	
	public void delete(String path) {
		try {
			zk.delete(path, -1);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (KeeperException e) {
			e.printStackTrace();
		}
	}
}
