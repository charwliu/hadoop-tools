package org.streamspf.hadoop.util;

import com.esotericsoftware.reflectasm.FieldAccess;
import com.esotericsoftware.reflectasm.MethodAccess;
import org.streamspf.hadoop.hbase.exceptions.HTableDefException;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Field;

public class Fields {

	public static Field getDeclaredField(Class<?> clazz, String name)
			throws HTableDefException {
		try {
			return clazz.getDeclaredField(name);
		} catch (Exception e) {
			throw new HTableDefException(name + " cannot accessed", e);
		}
	}

	public static Object getFieldValue(MethodAccess methodAccess, FieldAccess fieldAccess, Object target, Field field) {
		try {
			String prefix = (field.getType() == Boolean.TYPE) || (field.getType() == Boolean.class)
					? "is" : "get";
			String methodname = prefix + StringUtils.capitalize(field.getName());
			return methodAccess.invoke(target, methodname);
		} catch (IllegalArgumentException ex) {
			try {
				return fieldAccess.get(target, field.getName());
			} catch (IllegalArgumentException ignored) {
			}
		}
		return null;
	}

	public static void setFieldValue(MethodAccess methodAccess, FieldAccess fieldAccess, Object target, Field field, Object value) {
		try {
			if ((value == null) && (field.getType().isPrimitive())) {
				return;
			}

			String methodname = "set" + StringUtils.capitalize(field.getName());
			methodAccess.invoke(target, methodname, value);
		} catch (IllegalArgumentException ex) {
			try {
				fieldAccess.set(target, field.getName(), value);
			} catch (IllegalArgumentException ignored) {
			}
		}
	}
}
