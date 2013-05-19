package org.streamspf.hadoop.hbase.cascade;

import org.streamspf.hadoop.hbase.DaoOption;
import org.streamspf.hadoop.hbase.DefaultHBaseDao;
import org.streamspf.hadoop.hbase.HBaseDao;
import org.streamspf.hadoop.hbase.exceptions.HBaseDaoException;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

public class MainTableTest {
    @Test
    public void test() throws HBaseDaoException {
        MainTable mainTable = new MainTable();
        mainTable.setId("ABCD");
        mainTable.setName("计费");

        Other other = new Other();
        other.setId("ABCD");
        other.setColor("实时");

        mainTable.setOther(other);

        List subItems = new ArrayList();
        SubItem subItem = new SubItem();
        subItems.add(subItem);
        subItem.setId("ABCD");
        subItem.setSeq(0);
        subItem.setItemName("采集");

        subItem = new SubItem();
        subItems.add(subItem);
        subItem.setId("ABCD");
        subItem.setSeq(1);
        subItem.setItemName("预处理");

        subItem = new SubItem();
        subItems.add(subItem);
        subItem.setId("ABCD");
        subItem.setSeq(2);
        subItem.setItemName("批价");

        mainTable.setSubItems(subItems);

        List subItems2 = new ArrayList();
        SubItem2 subItem2 = new SubItem2();
        subItem2.setId("ABCD");
        subItem2.setSeq(0);
        subItem2.setItemName("语音");
        subItems2.add(subItem2);

        subItem2 = new SubItem2();
        subItem2.setId("ABCD");
        subItem2.setSeq(1);
        subItem2.setItemName("数据");
        subItems2.add(subItem2);
        mainTable.setSubItems2(subItems2);

        HBaseDao dao = new DefaultHBaseDao();
        dao.put(mainTable, EnumSet.of(DaoOption.CASCADE));

        MainTable mainTable2 = dao.get(MainTable.class, "ABCD", EnumSet.of(DaoOption.CASCADE));
        Assert.assertEquals(mainTable, mainTable2);

        mainTable2 = dao.get(MainTable.class, "ABCD");
        Assert.assertNull(mainTable2.getSubItems());
        Assert.assertNull(mainTable2.getSubItems2());
        Assert.assertNull(mainTable2.getOther());

        dao.delete(EnumSet.of(DaoOption.CASCADE), MainTable.class, "ABCD");
    }
}
