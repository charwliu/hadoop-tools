package org.streamspf.hadoop.hbase;

import org.streamspf.hadoop.hbase.exceptions.HBaseDaoException;
import org.streamspf.hadoop.hbase.exceptions.HTableDefException;
import java.util.EnumSet;
import java.util.List;

public interface HBaseDao {

	public <T> void put(T bean) throws HBaseDaoException;

	public <T> void put(List<T> beans) throws HBaseDaoException;

	public <T> boolean insert(T bean) throws HBaseDaoException;

	public <T> boolean update(T bean) throws HBaseDaoException;

	public <T> void put(T bean, EnumSet<DaoOption> options) throws HBaseDaoException;

	public <T> void put(List<T> beans, EnumSet<DaoOption> options) throws HBaseDaoException;

	public <T> boolean insert(T bean, EnumSet<DaoOption> options) throws HBaseDaoException;

	public <T> boolean update(T bean, EnumSet<DaoOption> options) throws HBaseDaoException;

	public <T> List<T> query(Class<T> beanClass, Object startRowkey, Object stopRowkey) throws HBaseDaoException;

	public <T> List<T> query(Class<T> beanClass, Object startRowkey, Object stopRowkey, int maxRows) throws HBaseDaoException;

	public <T> List<T> query(Class<T> beanClass, Object startRowkey, Object stopRowkey, int maxRows, EnumSet<DaoOption> options) throws HBaseDaoException;

	public <T> T get(Class<T> beanClass, Object rowkey, EnumSet<DaoOption> options) throws HBaseDaoException;

	public <T> T get(Class<T> beanClass, Object rowkey) throws HBaseDaoException;

	public <T> T get(Class<T> beanClass, Object rowkey, String family, String[] families) throws HBaseDaoException;

	public <T, V> V get(Class<T> beanClass, Object rowkey, Class<V> valueType, Object key) throws HBaseDaoException;

	public <T, V> V get(String family, Class<T> beanClass, Object rowkey, Class<V> valueType, Object key) throws HBaseDaoException;

	public <T> void put(Class<T> beanClass, Object rowkey, Object key, Object value, Object[] kvs) throws HBaseDaoException;

	public <T> void put(String family, Class<T> beanClass, Object rowkey, Object key, Object value, Object[] kvs) throws HBaseDaoException;

	public <T> void delete(Class<T> beanClass, Object rowKey) throws HBaseDaoException;

	public <T> void delete(EnumSet<DaoOption> options, Class<T> beanClass, Object rowkey) throws HBaseDaoException;

	public <T> void delete(String family, Class<T> beanClass, Object rowkey, Object key, Object[] keys) throws HBaseDaoException;

	public <T> void delete(Class<T> beanClass, Object rowkey, Object key, Object[] keys) throws HBaseDaoException;

	public <T> void trunc(Class<T> beanClass) throws HTableDefException;
}
