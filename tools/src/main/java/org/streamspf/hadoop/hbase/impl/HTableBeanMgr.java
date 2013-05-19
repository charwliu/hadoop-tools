package org.streamspf.hadoop.hbase.impl;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.streamspf.hadoop.hbase.annotations.*;
import org.streamspf.hadoop.hbase.exceptions.HTableDefException;
import org.streamspf.hadoop.util.Clazz;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public final class HTableBeanMgr {

    private static volatile HashMap<Class<?>, HTableBean> cache = new HashMap<Class<?>, HTableBean>();
    private static volatile HashSet<String> tableExistanceCheckCache = new HashSet<String>();

    public static <T> HTableBean getBean(String hbaseInstanceName, Class<T> beanClass) throws HTableDefException {
        return getBean(hbaseInstanceName, null, beanClass);
    }

    public static <T, P> HTableBean getBean(String hbaseInstanceName, Class<P> parentClass,
                                            Class<T> beanClass) throws HTableDefException {
        HTableBean hTableBean = cache.get(beanClass);
        if (hTableBean != null) {
            return hTableBean;
        }

        synchronized (cache) {
            HBaseTable hTable = beanClass.getAnnotation(HBaseTable.class);
            if (hTable == null) {
                throw new HTableDefException(beanClass + " is not annotationed by HTable");
            }

            checkTableExistence(hbaseInstanceName, hTable, beanClass);

            hTableBean = new HTableBean();
            hTableBean.setHBaseTable(beanClass);

            for (Field field : beanClass.getDeclaredFields()) {
                processHRowkey(hTableBean, field);
                processHRowkeyPart(hTableBean, field);
                processHColumn(hTableBean, field);
                processHDynamic(hTableBean, field);
                processHCascade(hbaseInstanceName, beanClass, hTableBean, field);
                processHParent(parentClass, hTableBean, field);
            }

            checkAnnotion(beanClass, hTableBean);
            hTableBean.afterPropertiesSet();
            cache.put(beanClass, hTableBean);
        }

        return hTableBean;
    }

    private static <T> void processHParent(Class<T> parentClass, HTableBean hTableBean, Field field)
            throws HTableDefException {
        HParent hParent = field.getAnnotation(HParent.class);
        if (hParent == null) {
            return;
        }

        if (hTableBean.getHParentField() != null) {
            throw new HTableDefException("@HParent can only define on no more than one field.");
        }

        if ((parentClass != null) && (field.getType() != parentClass)) {
            throw new HTableDefException("@HParent can only define on the field whose type is same with its parent.");
        }

        hTableBean.setHParent(field);
    }

    private static <T> void processHCascade(String hbaseInstanceName, Class<T> beanClass, HTableBean hTableBean, Field field) throws HTableDefException {
        if (field.getAnnotation(HCascade.class) == null) return;

        Class clazz = getCascadeClass(field);
        if (clazz == Void.class) throw new HTableDefException("@Cascade cannot detect its clazz property.");

        getBean(hbaseInstanceName, beanClass, clazz);
        hTableBean.addHRelateTo(field);
    }

    public static Class<?> getCascadeClass(Field field) {
        Class<?> clazz = field.getAnnotation(HCascade.class).clazz();
        if (clazz == Void.class) {
            if ((List.class.isAssignableFrom(field.getType()))
                    && (ParameterizedType.class.isAssignableFrom(field.getGenericType().getClass()))) {
                ParameterizedType genericType = (ParameterizedType) field.getGenericType();
                clazz = (Class) genericType.getActualTypeArguments()[0];
            } else {
                clazz = field.getType();
            }
        }

        return clazz;
    }

    private static void checkAnnotion(Class<?> beanClass, HTableBean hTableBean) throws HTableDefException {
        if ((hTableBean.getHRowkeyField() == null) && (hTableBean.getHRowkeyPartFields().isEmpty())) {
            throw new HTableDefException(beanClass + " does not define a @HRowkey or @HRowkeyPart field.");
        }
        if ((hTableBean.getHColumnFields().isEmpty()) && (hTableBean.getHDynamicFields().isEmpty())) {
            throw new HTableDefException(beanClass + " should define at least one @HColumn or @HDynamic filed.");
        }
    }

    private static void processHDynamic(HTableBean hTableBean, Field field) throws HTableDefException {
        HDynamic hdynamic = field.getAnnotation(HDynamic.class);
        if (hdynamic == null) {
            return;
        }

        if (!Map.class.isAssignableFrom(field.getType())) {
            throw new HTableDefException("@HDynamic can only defined on Map object");
        }

        HBaseTable hbaseTable = hTableBean.getBeanClass().getAnnotation(HBaseTable.class);
        checkFamily(field.getName(), hdynamic.family(), hbaseTable.families());
        hTableBean.addHDynamic(field);
    }

    private static void processHColumn(HTableBean hTableBean, Field field) throws HTableDefException {
        HColumn hcolumn = field.getAnnotation(HColumn.class);
        if (hcolumn == null) {
            return;
        }

        HBaseTable hbaseTable = hTableBean.getBeanClass().getAnnotation(HBaseTable.class);
        checkFamily(field.getName(), hcolumn.family(), hbaseTable.families());
        hTableBean.addHColumn(field);
    }

    private static void checkFamily(String fieldName, String family, String[] families) throws HTableDefException {
        if ((StringUtils.isEmpty(family)) && ((families == null) || (families.length == 0) || (StringUtils.isEmpty(families[0])))) {
            throw new HTableDefException(fieldName + " does not define a family");
        }
    }

    private static void processHRowkey(HTableBean hTableBean, Field field) throws HTableDefException {
        HRowkey hrowkey = field.getAnnotation(HRowkey.class);
        if (hrowkey == null) {
            return;
        }

        if (hTableBean.getHRowkeyField() != null) {
            throw new HTableDefException("@HRowkey can only define on no more than one field.");
        }
        if (hTableBean.getHRowkeyPartFields().size() > 0) {
            throw new HTableDefException("@HRowkey can not defined along with @HRowkeyPart.");
        }

        hTableBean.setHRowkey(field);
    }

    private static void processHRowkeyPart(HTableBean hTableBean, Field field) throws HTableDefException {
        HRowkeyPart hRowkeyPart = field.getAnnotation(HRowkeyPart.class);
        if (hRowkeyPart != null) {
            hTableBean.addHRowkeyPart(field);
        }
        if ((hTableBean.getHRowkeyPartFields().size() > 0) && (hTableBean.getHRowkeyField() != null)) {
            throw new HTableDefException("@HRowkey can not defined along with @HRowkeyPart.");
        }
    }

    private static void checkTableExistence(String hbaseInstanceName, HBaseTable hBaseTable, Class<?> beanClass) throws HTableDefException {
        String tableName = getTableName(hbaseInstanceName, hBaseTable, beanClass);
        if (hBaseTable.autoCreate()) {
            return;
        }
        checkAndCreateTable(hbaseInstanceName, hBaseTable, beanClass, tableName);
    }

    protected static void checkAndCreateTable(String hbaseInstanceName, HBaseTable hBaseTable, Class<?> beanClass, String tableName) throws HTableDefException {
        String cachedName = hbaseInstanceName + "$" + tableName;
        if (tableExistanceCheckCache.contains(cachedName)) {
            return;
        }

        synchronized (tableExistanceCheckCache) {
            if (tableExistanceCheckCache.contains(cachedName)) {
                return;
            }

            checkAndCreateTableWoCache(hbaseInstanceName, hBaseTable, tableName);
        }
    }

    protected static void checkAndCreateTableWoCache(String hbaseInstanceName, HBaseTable hBaseTable, String tableName) throws HTableDefException {
        HBaseAdmin admin = null;
        try {
            admin = HBaseAdminMgr.createAdmin(hbaseInstanceName);
            if (!admin.tableExists(tableName)) {
                if (!hBaseTable.autoCreate()) {
                    throw new HTableDefException(tableName + " does not exist");
                }
                if ((hBaseTable.families() == null) || (hBaseTable.families().length == 0)) {
                    throw new HTableDefException(tableName + " does not define its families");
                }
                HTableDescriptor tableDesc = new HTableDescriptor(tableName);
                for (String fam : hBaseTable.families()) {
                    tableDesc.addFamily(new HColumnDescriptor(fam));
                }
                admin.createTable(tableDesc);
            } else if (!admin.isTableEnabled(tableName)) {
                throw new HTableDefException(tableName + " is not enabled");
            }

            tableExistanceCheckCache.add(hbaseInstanceName + "$" + tableName);
        } catch (Exception e) {
            throw new HTableDefException(e);
        } finally {
            HBaseAdminMgr.close(admin);
        }
    }

    public static String getTableName(String hbaseInstanceName, HBaseTable hbaseTable, Class<?> beanClass) throws HTableDefException {
        String tableName;
        if (hbaseTable.nameCreator() != Void.class) {
            Object nameCreator = Clazz.newInstance(hbaseTable.nameCreator());
            Method method = findProperMethod(nameCreator.getClass());
            if (method == null) {
                throw new HTableDefException("no proper method found for " + hbaseTable.nameCreator());
            }
            tableName = invokeMethod(method, nameCreator, hbaseTable.name());
        } else {
            tableName = hbaseTable.name();
        }
        if (StringUtils.isEmpty(tableName))  {
            throw new HTableDefException(hbaseTable.nameCreator() + " create an empty name");
        }
        if (hbaseTable.autoCreate())  {
            checkAndCreateTable(hbaseInstanceName, hbaseTable, beanClass, tableName);
        }
        return tableName;
    }

    public static Method findProperMethod(Class<?> clazz) {
        Method[] methods = clazz.getMethods();
        for (Method method : methods) {
            if (!Modifier.isStatic(method.getModifiers())) {
                Class[] parameterTypes = method.getParameterTypes();
                if ((parameterTypes.length == 1) && (parameterTypes[0] == String.class)
                        && (method.getReturnType() == String.class)) {
                    return method;
                }
            }
        }
        return null;
    }

    public static String invokeMethod(Method method, Object nameCreator, String name) throws HTableDefException {
        try {
            return (String) method.invoke(nameCreator, name);
        } catch (Exception e) {
            throw new HTableDefException(e);
        }
    }
}