package org.streamspf.hadoop.hbase.impl;

import org.streamspf.hadoop.CallRecordDetail;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

import java.util.HashSet;
import java.util.Set;

public class HTableBeanTest {

    HTableBean hTableBean;

    @Before
    public void setUp() throws Exception {
        hTableBean = new HTableBean();
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testGetKnownRowkeyPartsBytesLen() throws Exception {

    }

    @Test
    public void testGetUnkownRowkeyPartsBytesLen() throws Exception {

    }

    @Test
    public void testAfterPropertiesSet() throws Exception {

    }

    @Test
    public void testGetRowkey() throws Exception {

    }

    @Test
    public void testSetRowkey() throws Exception {

    }

    @Test
    public void testSetHBaseTable() throws Exception {

    }

    @Test
    public void testSetHRowkey() throws Exception {

    }

    @Test
    public void testAddHColumn() throws Exception {

    }

    @Test
    public void testAddHRowkeyPart() throws Exception {

    }

    @Test
    public void testAddHRelateTo() throws Exception {

    }

    @Test
    public void testGetHRowkeyField() throws Exception {

    }

    @Test
    public void testSetRowkeyField() throws Exception {

    }

    @Test
    public void testAddHDynamic() throws Exception {

    }

    @Test
    public void testBeanClass() throws Exception {
        hTableBean.setBeanClass(CallRecordDetail.class);
        assertEquals(CallRecordDetail.class, hTableBean.getBeanClass());

    }


    @Test
    public void testGetHColumnFields() throws Exception {

    }

    @Test
    public void testGetHDynamicFields() throws Exception {

    }

    @Test
    public void testFamilies() throws Exception {

        Set<String> families = new HashSet<String>();
        families.add("cf1");
        families.add("cf2");
        hTableBean.setFamilies(families);
        assertEquals(families, hTableBean.getFamilies());

        Set<byte[]> bfamilies = new HashSet<byte[]>();
        bfamilies.add("cf1".getBytes());
        bfamilies.add("cf2".getBytes());
        hTableBean.setBfamilies(bfamilies);
        assertEquals(bfamilies, hTableBean.getBfamilies());
    }

    @Test
    public void testGetHRowkeyPartFields() throws Exception {

    }

    @Test
    public void testGetHRelateToFields() throws Exception {

    }

    @Test
    public void testSetHRelateToFields() throws Exception {

    }

    @Test
    public void testGetHParentField() throws Exception {

    }

    @Test
    public void testSetHParent() throws Exception {

    }

    @Test
    public void testGetHBaseTable() throws Exception {

    }

    @Test
    public void testGetMethodAccess() throws Exception {

    }

    @Test
    public void testGetFieldAccess() throws Exception {

    }
}
