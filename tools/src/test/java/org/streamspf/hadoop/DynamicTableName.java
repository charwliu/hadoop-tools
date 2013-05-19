package org.streamspf.hadoop;

import org.streamspf.hadoop.hbase.annotations.HBaseTable;
import org.streamspf.hadoop.hbase.annotations.HColumn;
import org.streamspf.hadoop.hbase.annotations.HRowkey;
import org.streamspf.hadoop.hbase.impl.ContextNameCreator;

@HBaseTable(name = "basename", nameCreator = ContextNameCreator.class, autoCreate = true)
public class DynamicTableName {

	@HRowkey
	private long rowkey;

	@HColumn(key="n")
	private String name;

	public long getRowkey() {
		return this.rowkey;
	}

	public void setRowkey(long rowkey) {
		this.rowkey = rowkey;
	}

	public String getName() {
		return this.name;
	}

	public void setName(String name) {
		this.name = name;
	}
}
