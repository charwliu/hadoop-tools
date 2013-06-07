package org.streamspf.hadoop.hbase;

import org.streamspf.hadoop.hbase.exceptions.HBaseDaoException;
import java.util.EnumSet;
import java.util.List;

public abstract class AbstractBaseDao implements HBaseDao {

	public <T> void put(T bean) throws HBaseDaoException {
		put(bean, EnumSet.noneOf(DaoOption.class));
	}

	public <T> void put(List<T> beans) throws HBaseDaoException {
		put(beans, EnumSet.noneOf(DaoOption.class));
	}

	public <T> boolean insert(T bean) throws HBaseDaoException {
		return upsert(bean, true, EnumSet.noneOf(DaoOption.class));
	}

	public <T> boolean update(T bean) throws HBaseDaoException {
		 return upsert(bean, false, EnumSet.noneOf(DaoOption.class));
	}

	public <T> boolean insert(T bean, EnumSet<DaoOption> options) throws HBaseDaoException {
		return upsert(bean, true, options);
	}

	public <T> boolean update(T bean, EnumSet<DaoOption> options) throws HBaseDaoException {
		return upsert(bean, false, options);
	}

	public <T> List<T> query(Class<T> beanClass, Object startRowkey, Object stopRowkey) throws HBaseDaoException {
		 return query(beanClass, startRowkey, stopRowkey, 0);
	}

	public <T> List<T> query(Class<T> beanClass, Object startRowkey, Object stopRowkey, int maxRows) throws HBaseDaoException {
		 return queryImpl(beanClass, startRowkey, stopRowkey, maxRows, EnumSet.noneOf(DaoOption.class));
	}

	public <T> List<T> query(Class<T> beanClass, Object startRowkey, Object stopRowkey, int maxRows, EnumSet<DaoOption> options) throws HBaseDaoException {
		 return queryImpl(beanClass, startRowkey, stopRowkey, maxRows, options);
	}

	public <T> T get(Class<T> beanClass, Object rowkey, EnumSet<DaoOption> options) throws HBaseDaoException {
		 return getImpl(beanClass, options, rowkey);
	}

	public <T> T get(Class<T> beanClass, Object rowkey) throws HBaseDaoException {
		return getImpl(beanClass, EnumSet.noneOf(DaoOption.class), rowkey);
	}

	public <T, V> V get(Class<T> beanClass, Object rowkey, Class<V> valueType, Object key) throws HBaseDaoException {
		return get("", beanClass, rowkey, valueType, key);
	}

	public <T> void put(Class<T> beanClass, Object rowkey, Object key, Object value, Object[] kvs) throws HBaseDaoException {
		put("", beanClass, rowkey, key, value, kvs);
	}

	public <T> void delete(Class<T> beanClass, Object rowkey) throws HBaseDaoException {
		delete(EnumSet.noneOf(DaoOption.class), beanClass, rowkey);
	}

	public <T> void delete(Class<T> beanClass, Object rowkey, Object key, Object[] keys) throws HBaseDaoException {
		delete("", beanClass, rowkey, key, keys);
	}

	protected abstract <T> T getImpl(Class<T> beanClass, EnumSet<DaoOption> options, Object bean) throws HBaseDaoException;

	protected abstract <T> T getImpl(Class<T> beanClass, Object bean, String family, String[] families) throws HBaseDaoException;

	protected abstract <T> List<T> queryImpl(Class<T> beanClass, Object startRowkey, Object stopRowkey, int maxRows, EnumSet<DaoOption> options) throws HBaseDaoException;

	protected abstract <T> boolean upsert(T bean, boolean isInsert, EnumSet<DaoOption> options) throws HBaseDaoException;
}
