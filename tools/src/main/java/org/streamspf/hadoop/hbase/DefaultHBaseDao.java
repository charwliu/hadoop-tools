package org.streamspf.hadoop.hbase;

import com.google.common.io.Closeables;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.streamspf.hadoop.hbase.annotations.*;
import org.streamspf.hadoop.hbase.exceptions.EmptyValueException;
import org.streamspf.hadoop.hbase.exceptions.FamilyEmptyException;
import org.streamspf.hadoop.hbase.exceptions.HBaseDaoException;
import org.streamspf.hadoop.hbase.exceptions.HTableDefException;
import org.streamspf.hadoop.hbase.impl.HBaseAdminMgr;
import org.streamspf.hadoop.hbase.impl.HTableBean;
import org.streamspf.hadoop.hbase.impl.HTableBeanMgr;
import org.streamspf.hadoop.hbase.pool.HTablePoolManager;
import org.streamspf.hadoop.util.Clazz;
import org.streamspf.hadoop.util.Fields;
import org.streamspf.hadoop.util.Hex;
import org.streamspf.hadoop.util.Types;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;

import static org.streamspf.hadoop.hbase.impl.HTableBeanMgr.getBean;
import static org.streamspf.hadoop.hbase.impl.HTableBeanMgr.getCascadeClass;

public class DefaultHBaseDao extends AbstractBaseDao {

    private static final int FETCH_ROWS = 1000;
    private String hbaseInstanceName;

    public DefaultHBaseDao() {
        this("default");
    }

    public DefaultHBaseDao(String hbaesInstanceName) {
        this.hbaseInstanceName = hbaesInstanceName;
    }

    private void addKeyAndFirstKeyOnlyFilter(Get get) {
        FilterList filterList = new FilterList();
        filterList.addFilter(new KeyOnlyFilter());
        filterList.addFilter(new FirstKeyOnlyFilter());
        get.setFilter(filterList);
    }

    public <T> void delete(String family, Class<T> beanClass, Object rowkey, Object key, Object[] keys)
            throws HBaseDaoException {
        HTableBean tableBean = getBean(this.hbaseInstanceName, beanClass);
        byte[] bRowkey = tableBean.getRowkey(rowkey);
        Delete delete = new Delete(bRowkey);
        createDeleteKeys(getDefaultFamily(tableBean, family), delete, key, keys);
        commitDelete(tableBean.getHBaseTable(), delete, beanClass);
    }

    private void createDeleteKeys(String family, Delete delete, Object key, Object[] keys) {
        byte[] famBytes = Bytes.toBytes(family);
        delete.deleteColumn(famBytes, Types.toBytes(key));
        for (Object key1 : keys) {
            delete.deleteColumn(famBytes, Types.toBytes(key1));
        }
    }

    @Override
    protected <T> T getImpl(Class<T> beanClass, EnumSet<DaoOption> options, Object bean) throws HBaseDaoException {
        HTableBean tableBean = getBean(this.hbaseInstanceName, beanClass);
        byte[] bRowkey = tableBean.getRowkey(bean);
        return getImpl(bRowkey, beanClass, options);
    }

    private <T> T getImpl(byte[] bRowkey, Class<T> beanClass, EnumSet<DaoOption> options) throws HBaseDaoException {
        HTableBean tableBean = getBean(this.hbaseInstanceName, beanClass);
        Get get = new Get(bRowkey);
        Result rs = getValues(tableBean, get);
        if (rs.isEmpty()) {
            return null;
        }

        T retBean = Clazz.newInstance(beanClass);
        tableBean.setRowkey(retBean, bRowkey);
        setValues(bRowkey, retBean, tableBean, rs, options);
        return retBean;
    }

    protected <T> T getImpl(Class<T> beanClass, Object bean, String family, String[] families) throws HBaseDaoException {
        HTableBean tableBean = getBean(this.hbaseInstanceName, beanClass);
        byte[] bRowkey = tableBean.getRowkey(bean);

        Get get = new Get(bRowkey);
        Result rs = getValues(tableBean, get, family, families);
        if (rs.isEmpty()) {
            return null;
        }

        T retBean = Clazz.newInstance(beanClass);
        tableBean.setRowkey(retBean, bRowkey);

        setValues(retBean, tableBean, rs, family, families);
        return retBean;
    }

    @Override
    public <T> T get(Class<T> beanClass, Object rowkey, String family, String[] families) throws HBaseDaoException {
        return getImpl(beanClass, rowkey, family, families);
    }

    @Override
    protected <T> List<T> queryImpl(Class<T> beanClass, Object startRowkey, Object stopRowkey, int maxRows, EnumSet<DaoOption> options) throws HBaseDaoException {
        HTableBean tableBean = getBean(this.hbaseInstanceName, beanClass);
        byte[] startRow = tableBean.getRowkey(startRowkey);
        byte[] stopRow = stopRowkey != null ? tableBean.getRowkey(stopRowkey) : null;
        return queryImpl(beanClass, startRow, stopRow, maxRows, options);
    }

    private <T> List<T> queryImpl(Class<T> beanClass, byte[] startRow, byte[] stopRow, int maxRows, EnumSet<DaoOption> options)
            throws HBaseDaoException {
        HTableBean tableBean = getBean(this.hbaseInstanceName, beanClass);
        Scan scan = stopRow != null ? new Scan(startRow, stopRow) : new Scan(startRow);
        HTableInterface hTable = null;
        try {
            for (byte[] family : tableBean.getBfamilies()) {
                scan.addFamily(family);
            }

            hTable = getHTable(tableBean);
            ResultScanner rr = hTable.getScanner(scan);

            int rows = 0;
            ArrayList<T> arrayList = new ArrayList<T>();
            for (Result[] rss = rr.next(FETCH_ROWS); (rss != null) && (rss.length > 0); rss = rr.next(FETCH_ROWS)) {
                for (Result rs : rss) {
                    T retBean = Clazz.newInstance(beanClass);
                    tableBean.setRowkey(retBean, rs.getRow());
                    setValues(rs.getRow(), retBean, tableBean, rs, options);
                    arrayList.add(retBean);
                    if (maxRows > 0) {
                        rows++;
                        if (rows == maxRows) {
                            break;
                        }
                    }
                }
            }
            return arrayList;
        } catch (IOException e) {
            throw new HBaseDaoException(e);
        } finally {
            closeHTable(hTable);
        }
    }

    @Override
    protected <T> boolean upsert(T bean, boolean ifInsertElseUpdate, EnumSet<DaoOption> options) throws HBaseDaoException {
        HTableBean tableBean = getBean(this.hbaseInstanceName, bean.getClass());
        byte[] bRowkey = tableBean.getRowkey(bean);
        Get get = new Get(bRowkey);
        addKeyAndFirstKeyOnlyFilter(get);

        Result rs = getValues(tableBean, get);
        if ((rs.isEmpty() ^ ifInsertElseUpdate)) {
            return false;
        }

        Put put = new Put(bRowkey);
        createPutValues(bean, tableBean, put, options);
        commitPut(tableBean, put);
        return true;
    }

    public <T> void trunc(Class<T> beanClass) throws HTableDefException {
        HTableBean tableBean = getBean(this.hbaseInstanceName, beanClass);
        HBaseAdmin admin = null;
        try {
            admin = HBaseAdminMgr.createAdmin(this.hbaseInstanceName);
            String tableName = getTableName(tableBean.getHBaseTable(), beanClass);
            HTableDescriptor tableDescriptor = admin.getTableDescriptor(Bytes.toBytes(tableName));

            if (!admin.isTableDisabled(tableName)) {
                admin.disableTable(tableName);
            }

            admin.deleteTable(tableName);
            admin.createTable(tableDescriptor);
        } catch (Exception e) {
            throw new HTableDefException(e);
        } finally {
            Closeables.closeQuietly(admin);
        }
    }

    private void commitPut(HTableBean tableBean, Put put) throws HBaseDaoException {
        HTableInterface hTable = null;
        try {
            hTable = getHTable(tableBean);
            hTable.put(put);
            hTable.flushCommits();
        } catch (IOException e) {
            throw new HBaseDaoException(e);
        } finally {
            closeHTable(hTable);
        }
    }

    private void commitPut(HTableBean tableBean, List<Put> put) throws HBaseDaoException {
        HTableInterface hTable = null;
        try {
            hTable = getHTable(tableBean);
            hTable.put(put);
            hTable.flushCommits();
        } catch (IOException e) {
            throw new HBaseDaoException(e);
        } finally {
            closeHTable(hTable);
        }
    }

    private Result getValues(HTableBean tableBean, Get get) throws HBaseDaoException {
        HTableInterface hTable = null;
        try {
            for (byte[] family : tableBean.getBfamilies()) {
                get.addFamily(family);
            }
            hTable = getHTable(tableBean);
            return hTable.get(get);
        } catch (IOException e) {
            throw new HBaseDaoException(e);
        } finally {
            closeHTable(hTable);
        }
    }

    private void commitDelete(HBaseTable htableAnn, Delete delete, Class<?> beanClass) throws HBaseDaoException {
        HTableInterface hTable = null;
        try {
            hTable = getHTable(htableAnn, beanClass);
            commitDelete(hTable, delete);
        } finally {
            closeHTable(hTable);
        }
    }

    private void commitDelete(HTableInterface hTable, Delete delete) throws HBaseDaoException {
        try {
            hTable.delete(delete);
            hTable.flushCommits();
        } catch (IOException e) {
            throw new HBaseDaoException(e);
        } finally {
            closeHTable(hTable);
        }
    }

    private void closeHTable(HTableInterface hTable) {
        if (hTable != null) {
            try {
                hTable.close();
            } catch (IOException ignored) {
            }
        }
    }

    public <T> void put(T bean, EnumSet<DaoOption> options) throws HBaseDaoException {
        HTableBean tableBean = getBean(this.hbaseInstanceName, bean.getClass());
        byte[] bRowkey = tableBean.getRowkey(bean);

        Put put = new Put(bRowkey);
        createPutValues(bean, tableBean, put, options);
        commitPut(tableBean, put);
    }

    public <T> void put(List<T> beans, EnumSet<DaoOption> options) throws HBaseDaoException {
        if ((beans == null) || (beans.isEmpty())) {
            return;
        }

        HTableBean tableBean = getBean(this.hbaseInstanceName, beans.get(0).getClass());
        ArrayList<Put> putsList = new ArrayList<Put>(beans.size());
        for (T bean : beans) {
            byte[] bRowkey = tableBean.getRowkey(bean);
            Put put = new Put(bRowkey);
            createPutValues(bean, tableBean, put, options);
            putsList.add(put);
        }

        commitPut(tableBean, putsList);
    }

    public <T, V> V get(String family, Class<T> beanClass, Object rowkey, Class<V> valueType, Object key) throws HBaseDaoException {
        HTableBean tableBean = getBean(this.hbaseInstanceName, beanClass);
        byte[] bRowkey = tableBean.getRowkey(rowkey);

        Get get = new Get(bRowkey);
        byte[] bkey = Types.toBytes(key);
        byte[] bfamily = Bytes.toBytes(getDefaultFamily(tableBean, family));
        Result rs = getSingleValue(tableBean, get, bfamily, bkey);
        if (rs.isEmpty()) {
            return null;
        }

        return Types.fromBytes(rs.getValue(bfamily, bkey), valueType);
    }

    public <T> void put(String family, Class<T> beanClass, Object rowkey, Object key, Object value, Object[] kvs) throws HBaseDaoException {
        HTableBean tableBean = getBean(this.hbaseInstanceName, beanClass);
        byte[] bRowkey = tableBean.getRowkey(rowkey);
        byte[] bfamily = Bytes.toBytes(getDefaultFamily(tableBean, family));
        Put put = new Put(bRowkey);
        createPutKv(put, bfamily, key, value, kvs);
        commitPut(tableBean, put);
    }

    private void createPutKv(Put put, byte[] family, Object key, Object value, Object[] kvs) {
        put.add(family, Types.toBytes(key), Types.toBytes(value));
        for (int i = 0; i < kvs.length; i += 2) {
            if (i + 1 < kvs.length) {
                put.add(family, Types.toBytes(kvs[i]), Types.toBytes(kvs[(i + 1)]));
            }
        }
    }

    private <T> void delete(byte[] bRowkey, EnumSet<DaoOption> options, Class<T> beanClass) throws HBaseDaoException {
        HTableBean tableBean = getBean(this.hbaseInstanceName, beanClass);
        Delete delete = new Delete(bRowkey);
        commitDelete(tableBean.getHBaseTable(), delete, beanClass);

        if (options.contains(DaoOption.CASCADE)) {
            for (Field field : tableBean.getHRelateToFields()) {
                HCascade hRelateTo = field.getAnnotation(HCascade.class);
                if (List.class.isAssignableFrom(field.getType())) {
                    cascadeDeleteByQuery(bRowkey, options, field, hRelateTo);
                } else if (getBean(this.hbaseInstanceName, field.getType()) != null) {
                    delete(options, field.getType(), bRowkey);
                }
            }
        }
    }

    public <T> void delete(EnumSet<DaoOption> options, Class<T> beanClass, Object rowkey) throws HBaseDaoException {
        HTableBean tableBean = getBean(this.hbaseInstanceName, beanClass);
        byte[] bRowkey = tableBean.getRowkey(rowkey);
        delete(bRowkey, options, beanClass);
    }

    private String getDefaultFamily(HTableBean tableBean, String family) throws FamilyEmptyException {
        String retFamily = family;
        if (StringUtils.isEmpty(retFamily)) {
            String[] families = tableBean.getHBaseTable().families();
            retFamily = (families != null) && (families.length > 0) ? families[0] : null;
        }
        if (StringUtils.isEmpty(retFamily)) {
            throw new FamilyEmptyException();
        }
        return retFamily;
    }

    private void createPutValues(Object bean, HTableBean tableBean, Put put, EnumSet<DaoOption> options) throws HBaseDaoException {
        int kvNums = 0;
        for (Field field : tableBean.getHColumnFields()) {
            kvNums += processHColumnFields(bean, tableBean, put, field);
        }
        for (Field field : tableBean.getHDynamicFields()) {
            kvNums += processHDynamicFields(bean, tableBean, put, field);
        }

        if (options.contains(DaoOption.CASCADE)) {
            for (Field field : tableBean.getHRelateToFields()) {
                processHRelateToFields(tableBean, bean, field);
            }
        }

        if (kvNums == 0) {
            throw new EmptyValueException("There is no values to do in this operation");
        }
    }

    private void processHRelateToFields(HTableBean tableBean, Object bean, Field field) throws HBaseDaoException {
        Object relateToValue = getFieldValue(tableBean, bean, field);
        if (relateToValue == null) {
            return;
        }

        if (List.class.isAssignableFrom(field.getType())) {
            List list = (List) relateToValue;
            for (Object item : list) {
                put(item);
            }
        } else {
            put(relateToValue);
        }
    }

    private void setFieldValue(HTableBean tableBean, Object bean, Field field, Object value) {
        Fields.setFieldValue(tableBean.getMethodAccess(), tableBean.getFieldAccess(), bean, field, value);
    }

    private Object getFieldValue(HTableBean tableBean, Object bean, Field field) {
        return Fields.getFieldValue(tableBean.getMethodAccess(), tableBean.getFieldAccess(), bean, field);
    }

    private int processHDynamicFields(Object bean, HTableBean tableBean, Put put, Field field) throws HBaseDaoException {
        Map<Object, Object> value = (Map) getFieldValue(tableBean, bean, field);
        if ((value == null) || (value.isEmpty())) {
            return 0;
        }

        HDynamic hdynamic = field.getAnnotation(HDynamic.class);
        String family = getDefaultFamily(tableBean, hdynamic.family());
        for (Map.Entry<Object, Object> entry : value.entrySet()) {
            put.add(Bytes.toBytes(family), Types.toBytes(entry.getKey()), Types.toBytes(entry.getValue()));
        }

        return value.size();
    }

    private int processHColumnFields(Object bean, HTableBean tableBean, Put put, Field field) throws FamilyEmptyException {
        HColumn kvAnn = field.getAnnotation(HColumn.class);

        String keyValue = kvAnn.key();
        String key = StringUtils.isBlank(keyValue) ? field.getName() : keyValue;
        Object value = getFieldValue(tableBean, bean, field);
        if (value == null) {
            return 0;
        }

        String family = getDefaultFamily(tableBean, kvAnn.family());
        put.add(Bytes.toBytes(family), Bytes.toBytes(key), Types.toBytes(value));
        return 1;
    }

    private HTableInterface getHTable(HTableBean tableBean) {
        return HTablePoolManager.getHTable(getTableName(tableBean.getHBaseTable(), tableBean.getBeanClass()), this.hbaseInstanceName);
    }

    private HTableInterface getHTable(HBaseTable hBaseTable, Class<?> beanClass) {
        return HTablePoolManager.getHTable(getTableName(hBaseTable, beanClass), this.hbaseInstanceName);
    }

    private String getTableName(HBaseTable hBaseTable, Class<?> beanClass) {
        try {
            return HTableBeanMgr.getTableName(this.hbaseInstanceName, hBaseTable, beanClass);
        } catch (HTableDefException e) {
            throw new RuntimeException(e);
        }
    }

    private Result getSingleValue(HTableBean tableBean, Get get, byte[] family, byte[] key) throws HBaseDaoException {
        HTableInterface hTable = null;
        try {
            get.addFamily(family);
            get.setFilter(new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(key)));
            hTable = getHTable(tableBean);
            return hTable.get(get);
        } catch (IOException e) {
            throw new HBaseDaoException(e);
        } finally {
            closeHTable(hTable);
        }
    }

    private void setValues(byte[] bRowkey, Object bean, HTableBean tableBean, Result rs, EnumSet<DaoOption> options) throws HBaseDaoException {
        for (byte[] bfamily : tableBean.getBfamilies()) {
            String strFamily = Bytes.toString(bfamily);
            ArrayList<String> usedQualifiers = new ArrayList<String>();
            populateHColumn(bean, tableBean, rs, bfamily, strFamily, usedQualifiers);
            populateHDynamic(bean, tableBean, rs, bfamily, strFamily, usedQualifiers);
        }

        if (options.contains(DaoOption.CASCADE)) {
            for (Field field : tableBean.getHRelateToFields()) {
                HCascade hRelateTo = field.getAnnotation(HCascade.class);
                if (List.class.isAssignableFrom(field.getType())) {
                    processListRelateTo(tableBean, hRelateTo, bRowkey, bean, options, field);
                } else {
                    HTableBean beanAnn = getBean(this.hbaseInstanceName, field.getType());
                    if (beanAnn != null) {
                        Object relateToValue = getImpl(bRowkey, field.getType(), options);
                        setFieldValue(tableBean, bean, field, relateToValue);
                        HTableBean hRelateToAnn = getBean(this.hbaseInstanceName,
                                field.getType());
                        if (hRelateToAnn.getHParentField() != null) {
                            setFieldValue(hRelateToAnn, relateToValue, hRelateToAnn.getHParentField(), bean);
                        }
                    }
                }
            }
        }
    }

    private void populateHColumn(Object bean, HTableBean tableBean, Result rs, byte[] bfamily, String strFamily, ArrayList<String> usedQualifiers) throws FamilyEmptyException {
        for (Field field : tableBean.getHColumnFields()) {
            HColumn hcolumn = field.getAnnotation(HColumn.class);
            String family = getDefaultFamily(tableBean, hcolumn.family());
            if (family.equals(strFamily)) {
                String key = StringUtils.defaultIfBlank(hcolumn.key(), field.getName());
                byte[] qualifier = Bytes.toBytes(key);
                usedQualifiers.add(Hex.toHex(qualifier));
                byte[] value = rs.getValue(bfamily, qualifier);
                setFieldValue(tableBean, bean, field, Types.fromBytes(value, field.getType()));
            }
        }
    }

    private void populateHDynamic(Object bean, HTableBean tableBean, Result rs, byte[] bfamily, String strFamily, ArrayList<String> usedQualifiers) throws FamilyEmptyException {
        NavigableMap<byte[], byte[]> familyMap = rs.getFamilyMap(bfamily);
        for (Map.Entry<byte[], byte[]> entry : familyMap.entrySet()) {
            if (!usedQualifiers.contains(Hex.toHex(entry.getKey()))) {
                for (Field field : tableBean.getHDynamicFields()) {
                    HDynamic hdynamic = field.getAnnotation(HDynamic.class);
                    String family = getDefaultFamily(tableBean, hdynamic.family());
                    if (family.equals(strFamily)) {
                        HTypePair[] mapping = hdynamic.mapping();
                        for (HTypePair hTypePair : mapping) {
                            Object key = Types.fromBytes(entry.getKey(), hTypePair.keyType());
                            if (key != null) {
                                Object value = Types.fromBytes(entry.getValue(), hTypePair.valueType());
                                Map<Object, Object> map = (Map) getFieldValue(tableBean, bean, field);
                                if (map == null) {
                                    map = new HashMap<Object, Object>();
                                    setFieldValue(tableBean, bean, field, map);
                                }
                                map.put(key, value);
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    private void processListRelateTo(HTableBean tableBean, HCascade hRelateTo, byte[] bRowkey, Object bean, EnumSet<DaoOption> options, Field field) throws HTableDefException, HBaseDaoException {
        Class clazz = getCascadeClass(field);
        HTableBean hRelateToAnn = getBean(this.hbaseInstanceName, clazz);

        int rightBytesLen =
                hRelateToAnn.getUnkownRowkeyPartsBytesLen() <= 0
                        ? hRelateToAnn.getKnownRowkeyPartsBytesLen() - bRowkey.length : hRelateTo.rowkeyBytesLen() > 0
                        ? hRelateTo.rowkeyBytesLen() - bRowkey.length
                        : hRelateToAnn.getKnownRowkeyPartsBytesLen();
        byte[] padding = new byte[rightBytesLen];
        for (int i = 0; i < rightBytesLen; i++) {
            padding[i] = 0;
        }
        byte[] startRow = Bytes.add(bRowkey, padding);
        for (int i = 0; i < rightBytesLen; i++) {
            padding[i] = -1;
        }
        byte[] stopRow = Bytes.add(bRowkey, padding);

        List<Object> lstRelatedValue = queryImpl(clazz, startRow, stopRow, 0, options);
        if (hRelateToAnn.getHParentField() != null) {
            for (Object object : lstRelatedValue) {
                setFieldValue(hRelateToAnn, object, hRelateToAnn.getHParentField(), bean);
            }
        }

        List<Object> list = (List) getFieldValue(tableBean, bean, field);
        if (list == null) {
            setFieldValue(tableBean, bean, field, lstRelatedValue);
        } else {
            for (Object object : lstRelatedValue) {
                list.add(object);
            }
        }
    }

    private Result getValues(HTableBean tableBean, Get get, String family, String[] families) throws HBaseDaoException {
        HTableInterface hTable = null;
        try {
            get.addFamily(Bytes.toBytes(family));
            for (String fam : families) {
                get.addFamily(Bytes.toBytes(fam));
            }

            hTable = getHTable(tableBean);
            return hTable.get(get);
        } catch (IOException e) {
            throw new HBaseDaoException(e);
        } finally {
            closeHTable(hTable);
        }
    }

    private void setValues(Object bean, HTableBean tableBean, Result rs, String family, String[] families) throws FamilyEmptyException {
        for (byte[] bfamily : tableBean.getBfamilies()) {
            String strFamily = Bytes.toString(bfamily);
            if (isAllowedFamily(strFamily, family, families)) {
                ArrayList<String> usedQualifiers = new ArrayList<String>();
                populateHColumn(bean, tableBean, rs, bfamily, strFamily, usedQualifiers);
                populateHDynamic(bean, tableBean, rs, bfamily, strFamily, usedQualifiers);
            }
        }
    }

    private boolean isAllowedFamily(String strFamily, String family, String[] families) {
        if (StringUtils.equals(strFamily, family)) {
            return true;
        }
        for (String fam : families) {
            if (StringUtils.equals(strFamily, fam)) {
                return true;
            }
        }
        return false;
    }

    private void cascadeDeleteByQuery(byte[] bRowkey, EnumSet<DaoOption> options, Field field, HCascade hRelateTo) throws HBaseDaoException {
        Class clazz = getCascadeClass(field);

        HTableBean hRelateToAnn = getBean(this.hbaseInstanceName, clazz);

        int rightBytesLen =
                hRelateToAnn.getUnkownRowkeyPartsBytesLen() <= 0
                        ? hRelateToAnn.getKnownRowkeyPartsBytesLen() - bRowkey.length : hRelateTo.rowkeyBytesLen() > 0
                        ? hRelateTo.rowkeyBytesLen() - bRowkey.length
                        : hRelateToAnn.getKnownRowkeyPartsBytesLen();
        byte[] padding = new byte[rightBytesLen];
        for (int i = 0; i < rightBytesLen; i++) {
            padding[i] = 0;
        }
        byte[] startRow = Bytes.add(bRowkey, padding);
        for (int i = 0; i < rightBytesLen; i++) {
            padding[i] = -1;
        }
        byte[] stopRow = Bytes.add(bRowkey, padding);

        List<?> lstRelatedValue = queryImpl(clazz, startRow, stopRow, 0, options);
        for (Object object : lstRelatedValue) {
            delete(options, clazz, hRelateToAnn.getRowkey(object));
        }
    }
}
