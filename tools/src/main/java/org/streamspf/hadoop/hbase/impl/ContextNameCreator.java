package org.streamspf.hadoop.hbase.impl;

public class ContextNameCreator {

	private static ThreadLocal<String> suffix = new ThreadLocal<String>();

	public static void setSuffix(String suffixStr) {
		suffix.set(suffixStr);
	}

	public String tableName(String name) {
		if (suffix.get() == null) {
			return name;
		}

		return name + '_' + suffix.get();
	}
}
