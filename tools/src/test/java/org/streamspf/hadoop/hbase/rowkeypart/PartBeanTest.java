package org.streamspf.hadoop.hbase.rowkeypart;

import org.streamspf.util.MD5;
import org.streamspf.hadoop.hbase.DefaultHBaseDao;
import org.streamspf.hadoop.hbase.HBaseDao;
import org.streamspf.hadoop.hbase.exceptions.HBaseDaoException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PartBeanTest {
    @Test
    public void test1() throws HBaseDaoException {
        PartBean partBean = new PartBean();
        partBean.setCode("XXYY");
        partBean.setId(1);
        partBean.setValue("hello world");

        HBaseDao hDao = new DefaultHBaseDao();
        hDao.put(partBean);

        PartBean partBean2 = new PartBean();
        partBean2.setCode("XXYY");
        partBean2.setId(1);

        PartBean partBean3 = hDao.get(PartBean.class, partBean2);

        MD5 md5 = new MD5();

        md5.update("XXYY");
        System.out.println("code(md5): " + md5.asHex());
        System.out.println("code: " + partBean3.getCode());
        assertEquals(partBean.getValue(), partBean3.getValue());
        assertEquals(partBean.getId(), partBean3.getId());
        assertEquals(md5.asHex(), partBean3.getCode());

    }
}
