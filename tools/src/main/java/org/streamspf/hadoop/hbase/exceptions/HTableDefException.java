package org.streamspf.hadoop.hbase.exceptions;

public class HTableDefException extends HBaseDaoException {

	public HTableDefException() {
	}

	public HTableDefException(String message) {
		super(message);
	}

	public HTableDefException(String message, Throwable cause) {
		super(message, cause);
	}

	public HTableDefException(Throwable cause) {
		super(cause);
	}
}
