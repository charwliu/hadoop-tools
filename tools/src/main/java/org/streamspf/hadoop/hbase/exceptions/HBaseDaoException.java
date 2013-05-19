package org.streamspf.hadoop.hbase.exceptions;

public class HBaseDaoException extends Exception {

	public HBaseDaoException() {
	}

	public HBaseDaoException(String message) {
		super(message);
	}

	public HBaseDaoException(String message, Throwable cause) {
		super(message, cause);
	}

	public HBaseDaoException(Throwable cause) {
		super(cause);
	}
}
