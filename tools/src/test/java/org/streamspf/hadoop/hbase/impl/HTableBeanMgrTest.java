package org.streamspf.hadoop.hbase.impl;

import org.streamspf.hadoop.hbase.impl.ContextNameCreator;
import org.streamspf.hadoop.hbase.impl.HTableBeanMgr;
import org.streamspf.hadoop.hbase.annotations.HBaseTable;
import org.streamspf.hadoop.hbase.exceptions.HTableDefException;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;

public class HTableBeanMgrTest {
    @Test
    public void testGetTableName()
            throws HTableDefException {
        ContextNameCreator.setSuffix(null);
        String tableName = HTableBeanMgr.getTableName("default", A.class.getAnnotation(HBaseTable.class), A.class);
        Assert.assertEquals("aaa", tableName);

        ContextNameCreator.setSuffix("001");
        tableName = HTableBeanMgr.getTableName("default", A.class.getAnnotation(HBaseTable.class), A.class);
        Assert.assertEquals("aaa_001", tableName);
    }

    @Test
    public void testFindProperMethod() {
        Method method = HTableBeanMgr.findProperMethod(ContextNameCreator.class);
        Assert.assertEquals("tableName", method.getName());
    }

    @Test
    public void testInvokeMethod() throws HTableDefException {
        Method method = HTableBeanMgr.findProperMethod(ContextNameCreator.class);

        ContextNameCreator.setSuffix(null);
        String tableName = HTableBeanMgr.invokeMethod(method, new ContextNameCreator(), "aaa");
        Assert.assertEquals("aaa", tableName);

        ContextNameCreator.setSuffix("001");
        tableName = HTableBeanMgr.invokeMethod(method, new ContextNameCreator(), "aaa");
        Assert.assertEquals("aaa_001", tableName);
    }

    @HBaseTable(name = "aaa", nameCreator = ContextNameCreator.class)
    class A {

    }
}
