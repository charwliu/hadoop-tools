package org.streamspf.hadoop.util;

import org.streamspf.hadoop.hbase.annotations.HConnectionConfig;
import org.reflections.Reflections;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ConfigUtils {

	private final static Map<String, Method> configMap = new HashMap<String, Method>();

	static {
		Reflections reflections = new Reflections("org.streamspf.hadoop.config");
		Set<Class<?>> classes = reflections.getTypesAnnotatedWith(HConnectionConfig.class);
		for (Class<?> class1 : classes) {
			Method[] arrayOfMethod = class1.getMethods();
			for (Method method : arrayOfMethod) {
				Class<?>[] parameterTypes = method.getParameterTypes();
				if ((parameterTypes.length == 0)
						&& (Map.class.isAssignableFrom(method.getReturnType()))
						&& (method.isAnnotationPresent(HConnectionConfig.class))) {
					HConnectionConfig configAnn = method.getAnnotation(HConnectionConfig.class);

					configMap.put(configAnn.value(), method);
				}
			}
		}
	}

	public static Map<String, String> getConfig(String instanceName) {
		Method method = configMap.get(instanceName);
		if (method == null) {
			return null;
		}

		return invokeMethod(method);
	}

    @SuppressWarnings("unchecked")
	public static <T> T invokeMethod(Method method) {
		try {
			Object instance = null;
			if (!Modifier.isStatic(method.getModifiers())) {
				instance = Clazz.newInstance(method.getDeclaringClass());
			}
			return (T) method.invoke(instance);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
