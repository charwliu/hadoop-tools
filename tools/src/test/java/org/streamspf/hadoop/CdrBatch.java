package org.streamspf.hadoop;

import org.streamspf.hadoop.hbase.annotations.HBaseTable;
import org.streamspf.hadoop.hbase.annotations.HColumn;
import org.streamspf.hadoop.hbase.annotations.HRowkey;
import org.streamspf.hadoop.hbase.impl.ContextNameCreator;

@HBaseTable(name = "CdrBatch", nameCreator = ContextNameCreator.class, autoCreate = true)
public class CdrBatch {

	@HRowkey
	private long timestamp;
	@HColumn(key = "s")
	private long start;
	@HColumn(key = "e")
	private long end;

	public long getTimestamp() {
		return this.timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public long getStart() {
		return this.start;
	}

	public void setStart(long start) {
		this.start = start;
	}

	public long getEnd() {
		return this.end;
	}

	public void setEnd(long end) {
		this.end = end;
	}
}
