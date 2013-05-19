package org.streamspf.hadoop.hbase.simple;


import org.streamspf.hadoop.common.MD5;
import org.streamspf.hadoop.hbase.DefaultHBaseDao;
import org.streamspf.hadoop.hbase.HBaseDao;
import org.streamspf.hadoop.hbase.exceptions.HBaseDaoException;
import org.streamspf.hadoop.util.Hex;
import org.junit.Test;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class SimpleBeanTest {
    MD5 md5 = new MD5();
    @Test
    public void testPutAndGet()
            throws HBaseDaoException, NoSuchAlgorithmException {
        HBaseDao hdao = new DefaultHBaseDao();
        SimpleBean simpleBean = new SimpleBean();
        simpleBean.setRowkey("H10101");
        simpleBean.setAge(20);
        simpleBean.setAdult(true);
        simpleBean.setName("Bob");
        SimpleBean simpleBean2 = hdao.get(SimpleBean.class, "K10101");
        assertNull(simpleBean2);
        hdao.put(simpleBean);
        md5.update("H10101");
        System.out.println(md5.asHex());

        MessageDigest md = MessageDigest.getInstance("MD5");
        md.update("H10101".getBytes());
        System.out.println(Hex.toHex(md.digest()));

        simpleBean2 = hdao.get(SimpleBean.class, "H10101");
        System.out.println("age: " + simpleBean2.getAge());
        System.out.println("name: " + simpleBean2.getName());
        System.out.println("Rowkey: " + Hex.toHex(simpleBean2.getRowkey().getBytes()));
        System.out.println("isAdult: " + simpleBean2.isAdult());
        assertEquals(simpleBean, simpleBean2);
    }
}
