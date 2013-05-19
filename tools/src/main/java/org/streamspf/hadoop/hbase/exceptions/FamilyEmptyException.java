package org.streamspf.hadoop.hbase.exceptions;


public class FamilyEmptyException extends HBaseDaoException {

	public FamilyEmptyException() {
	}

	public FamilyEmptyException(String message) {
		super(message);
	}

	public FamilyEmptyException(String message, Throwable cause) {
		super(message, cause);
	}

	public FamilyEmptyException(Throwable cause) {
		super(cause);
	}

}
