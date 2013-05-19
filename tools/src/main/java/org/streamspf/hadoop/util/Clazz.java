package org.streamspf.hadoop.util;

public class Clazz {

	public static <T> T newInstance(Class<T> clz) {
		try {
			return clz.newInstance();
		} catch (InstantiationException e) {
			throw new RuntimeException(e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}
}
