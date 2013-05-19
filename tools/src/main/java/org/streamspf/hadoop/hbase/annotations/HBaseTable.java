package org.streamspf.hadoop.hbase.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface HBaseTable {

	public String name();

	public boolean autoCreate() default false;

	public Class<?> nameCreator() default Void.class;

	public String[] families() default {"cf"};
}