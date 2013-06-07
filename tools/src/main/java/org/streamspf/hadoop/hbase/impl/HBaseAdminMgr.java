package org.streamspf.hadoop.hbase.impl;

import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.streamspf.hadoop.hbase.exceptions.HTableDefException;
import org.streamspf.hadoop.hbase.pool.HTablePoolManager;

public class HBaseAdminMgr {

	public static HBaseAdmin createAdmin(String hbaseInstanceName) throws HTableDefException {
		try {
			return new HBaseAdmin(HTablePoolManager.getHBaseConfig(hbaseInstanceName));
		} catch (MasterNotRunningException e) {
			throw new HTableDefException(e);
		} catch (ZooKeeperConnectionException e) {
			throw new HTableDefException(e);
		}
    }

	public static void close(HBaseAdmin admin) {
		if (admin == null) {
			return;
		}
		HConnectionManager.deleteConnection(admin.getConfiguration(), true);
	}
}
