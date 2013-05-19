package org.streamspf.hadoop.hbase.exceptions;


public class RowkeyEmptyException extends HBaseDaoException {

	public RowkeyEmptyException() {
	}

	public RowkeyEmptyException(String message) {
		super(message);
	}

	public RowkeyEmptyException(String message, Throwable cause) {
		super(message, cause);
	}

	public RowkeyEmptyException(Throwable cause) {
		super(cause);
	}

}
