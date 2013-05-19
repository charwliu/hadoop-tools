package org.streamspf.hadoop.hbase.exceptions;


public class EmptyValueException extends HBaseDaoException {

	public EmptyValueException() {
	}

	public EmptyValueException(String message) {
		super(message);
	}

	public EmptyValueException(String message, Throwable cause) {
		super(message, cause);
	}

	public EmptyValueException(Throwable cause) {
		super(cause);
	}

}
