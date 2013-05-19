package org.streamspf.hadoop.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.google.common.collect.Maps;
import org.streamspf.hadoop.hbase.BytesConvertable;
import java.util.Date;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.hbase.util.Bytes;

public class Types {

	private final static Map<Class<?>, Class<?>> primitiveMap = Maps.newHashMap();
	private final static Map<Class<?>, BytesConvertable<?>> basicConverters = Maps.newHashMap();
	private final static Map<Class<?>, Integer> bytesLenMap = Maps.newHashMap();
	private static BytesConvertable jsonConverter = new BytesConvertable<Object>() {
		public byte[] toBytes(Object object) {
			String jsonString = JSON.toJSONString(object, new SerializerFeature[]{SerializerFeature.WriteClassName});
			return Bytes.toBytes(jsonString);
		}

		public Object fromBytes(byte[] bytes) {
			String jsonString = Bytes.toString(bytes);
			return JSON.parse(jsonString);
		}
	};

	static {
		bytesLenMap.put(Boolean.class, 1);
		bytesLenMap.put(Byte.class, 1);
		bytesLenMap.put(Character.class, 1);
		bytesLenMap.put(Short.class, 2);
		bytesLenMap.put(Integer.class, 4);
		bytesLenMap.put(Long.class, 8);
		bytesLenMap.put(Float.class, 4);
		bytesLenMap.put(Double.class, 8);
		bytesLenMap.put(Date.class, 8);

		primitiveMap.put(Boolean.TYPE, Boolean.class);
		primitiveMap.put(Byte.TYPE, Byte.class);
		primitiveMap.put(Character.TYPE, Character.class);
		primitiveMap.put(Short.TYPE, Short.class);
		primitiveMap.put(Integer.TYPE, Integer.class);
		primitiveMap.put(Long.TYPE, Long.class);
		primitiveMap.put(Float.TYPE, Float.class);
		primitiveMap.put(Double.TYPE, Double.class);

		basicConverters.put(Boolean.class, new BytesConvertable<Boolean>() {
			public byte[] toBytes(Boolean bool) {
				return Bytes.toBytes(bool);
			}

			public Boolean fromBytes(byte[] bytes) {
				return bytes.length == 1 ? Bytes.toBoolean(bytes) : null;
			}
		});

		basicConverters.put(Byte.class, new BytesConvertable<Byte>() {
			public byte[] toBytes(Byte b) {
				return new byte[]{b};
			}

			public Byte fromBytes(byte[] bytes) {
				return bytes.length == 1 ? bytes[0] : null;
			}
		});
		basicConverters.put(Character.class, new BytesConvertable<Character>() {
			public byte[] toBytes(Character object) {
				return Bytes.toBytes(new String(new char[]{object}));
			}

			public Character fromBytes(byte[] bytes) {
				return bytes.length == 1 ? Bytes.toString(bytes).charAt(0) : null;
			}
		});
		basicConverters.put(Short.class, new BytesConvertable<Short>() {
			public byte[] toBytes(Short object) {
				return Bytes.toBytes(object);
			}

			public Short fromBytes(byte[] bytes) {
				return bytes.length == 2 ? Bytes.toShort(bytes) : null;
			}
		});
		basicConverters.put(Integer.class, new BytesConvertable<Integer>() {
			public byte[] toBytes(Integer object) {
				return Bytes.toBytes(object);
			}

			public Integer fromBytes(byte[] bytes) {
				return bytes.length == 4 ? Bytes.toInt(bytes) : null;
			}
		});
		basicConverters.put(Long.class, new BytesConvertable<Long>() {
			public byte[] toBytes(Long object) {
				return Bytes.toBytes(object);
			}

			public Long fromBytes(byte[] bytes) {
				return bytes.length == 8 ? Bytes.toLong(bytes) : null;
			}
		});
		basicConverters.put(Float.class, new BytesConvertable<Float>() {
			public byte[] toBytes(Float object) {
				return Bytes.toBytes(object);
			}

			public Float fromBytes(byte[] bytes) {
				return bytes.length == 4 ? Bytes.toFloat(bytes) : null;
			}
		});
		basicConverters.put(Double.class, new BytesConvertable<Double>() {
			public byte[] toBytes(Double object) {
				return Bytes.toBytes(object);
			}

			public Double fromBytes(byte[] bytes) {
				return bytes.length == 8 ? Bytes.toDouble(bytes) : null;
			}
		});
		basicConverters.put(String.class, new BytesConvertable<String>() {
			public byte[] toBytes(String object) {
				return Bytes.toBytes(object);
			}

			public String fromBytes(byte[] bytes) {
				return Bytes.toString(bytes);
			}
		});
		basicConverters.put(Date.class, new BytesConvertable<Date>() {
			public byte[] toBytes(Date object) {
				return Bytes.toBytes(object.getTime());
			}

			public Date fromBytes(byte[] bytes) {
				return new Date(Bytes.toLong(bytes));
			}
		});
	}

	public static byte[] toBytes(Object value) {
		if (value == null) {
			return null;
		}

		if ((value instanceof byte[])) {
			return (byte[]) value;
		}

		Class<?> valueClass = value.getClass();

		BytesConvertable bytesConvertable = basicConverters.get(valueClass);
		if ((bytesConvertable == null) && ((value instanceof BytesConvertable))) {
			bytesConvertable = (BytesConvertable) value;
		}
		if (bytesConvertable == null) {
			bytesConvertable = jsonConverter;
		}

		return bytesConvertable.toBytes(value);
	}

	public static <T> int getBytesLen(Class<T> type) {
		Class<?> targetType = type;
		if (targetType.isPrimitive()) {
			targetType = primitiveMap.get(targetType);
		}

		Integer bytesLen = bytesLenMap.get(targetType);

		return bytesLen != null ? bytesLen : -1;
	}

	public static <T> T fromBytes(byte[] bytes, Class<T> type) {
		if (bytes == null) {
			return null;
		}

		if (type == byte[].class) {
		  return (T)bytes;
		}

		Class<T> targetType = type;
		if (targetType.isPrimitive()) {
			targetType = (Class<T>)primitiveMap.get(targetType);
		}

		BytesConvertable<T> bytesConvertable = (BytesConvertable<T>)basicConverters.get(targetType);
		if ((bytesConvertable == null) && (BytesConvertable.class.isAssignableFrom(type))) {
			try {
				bytesConvertable = (BytesConvertable) type.newInstance();
			} catch (InstantiationException ex) {
				Logger.getLogger(Types.class.getName()).log(Level.SEVERE, null, ex);
			} catch (IllegalAccessException ex) {
				Logger.getLogger(Types.class.getName()).log(Level.SEVERE, null, ex);
			}
		}
		if (bytesConvertable == null) {
			bytesConvertable = jsonConverter;
		}

		return bytesConvertable.fromBytes(bytes);
	}
}
