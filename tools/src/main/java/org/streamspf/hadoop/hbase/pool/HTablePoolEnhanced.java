package org.streamspf.hadoop.hbase.pool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public class HTablePoolEnhanced extends HTablePool {

    private Configuration config;

    public HTablePoolEnhanced(Configuration config, int maxSize) {
        super(config, maxSize);
        this.config = config;
    }

    public Configuration getConfig() {
        return this.config;
    }


	@Override
    public HTableInterface getTable(String tableName) {
        return (HTableInterface) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class[]{HTableInterface.class},
                new HTableProxy(super.getTable(tableName), this));
    }

    public static class HTableProxy implements InvocationHandler {
        private HTableInterface table;
        private HTablePool htablePool;

        public HTableProxy(HTableInterface table, HTablePool htablePool) {
            this.table = table;
            this.htablePool = htablePool;
        }

        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (!method.getName().equals("close")) return method.invoke(this.table, args);

            this.htablePool.putTable(this.table);

            return null;
        }
    }
}