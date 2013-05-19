package org.streamspf.hadoop.hbase.dynamic;


import org.streamspf.hadoop.hbase.DefaultHBaseDao;
import org.streamspf.hadoop.hbase.HBaseDao;
import org.streamspf.hadoop.hbase.exceptions.HBaseDaoException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DynamicBeanTest {
    HBaseDao dao;

    @Before
    public void setUp() {
        dao = new DefaultHBaseDao();
    }

    @After
    public void tearDown() throws HBaseDaoException {
        dao.delete(DynamicBean.class, "dynamictest001");
    }

    @Test
    public void test() throws HBaseDaoException {
        DynamicBean dynamicBean = new DynamicBean();
        dynamicBean.setRowkey("dynamictest001");
        dynamicBean.setAppid(30000L);
        dynamicBean.setDesc("It's dynamic columns test001");

        Map dynamicProperties = new HashMap();

        SignKey signKey = new SignKey();
        signKey.setEff(100L);
        signKey.setExp(2000L);

        dynamicProperties.put(signKey, "my signkey");

        ParamKey paramKey = new ParamKey();
        paramKey.setEff(111111L);
        paramKey.setExp(777777L);
        ParamKeyValue paramKeyValue = new ParamKeyValue();
        paramKeyValue.setSalt(333L);
        paramKeyValue.setSecurity("paramsecurity");
        dynamicProperties.put(paramKey, paramKeyValue);

        dynamicProperties.put("what", "this");

        dynamicBean.setDynamicProperties(dynamicProperties);

        dao = new DefaultHBaseDao();
        boolean ok = dao.insert(dynamicBean);
        assertTrue(ok);

        DynamicBean dynamicBean2 = dao.get(DynamicBean.class, "dynamictest001");
        assertEquals(dynamicBean, dynamicBean2);

    }
}
