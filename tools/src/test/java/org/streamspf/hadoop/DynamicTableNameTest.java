package org.streamspf.hadoop;

import org.junit.*;
import org.streamspf.hadoop.hbase.DefaultHBaseDao;
import org.streamspf.hadoop.hbase.HBaseDao;
import org.streamspf.hadoop.hbase.exceptions.HBaseDaoException;
import org.streamspf.hadoop.hbase.impl.ContextNameCreator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author liuwei
 */
public class DynamicTableNameTest {
    HBaseDao dao;

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
        dao = new DefaultHBaseDao();
    }

    @After
    public void tearDown() throws Exception {
        ContextNameCreator.setSuffix("01");
        dao.delete(DynamicTableName.class, 1L);
        ContextNameCreator.setSuffix("02");
        dao.delete(DynamicTableName.class, 1L);
    }

    @Test
    public void test1() throws HBaseDaoException {

        DynamicTableName bean = new DynamicTableName();
        bean.setRowkey(1L);
        bean.setName("aaa");


        ContextNameCreator.setSuffix("01");
        dao.insert(bean);

        ContextNameCreator.setSuffix("02");
        assertFalse(dao.update(bean));
        assertTrue(dao.insert(bean));
        DynamicTableName bean2 = dao.get(DynamicTableName.class, 1L);
        assertEquals("aaa", bean2.getName());
        assertFalse(dao.insert(bean));
        bean.setName("bbb");
        dao.update(bean);
        bean2 = dao.get(DynamicTableName.class, 1L);
        assertEquals("bbb", bean2.getName());
    }
}
