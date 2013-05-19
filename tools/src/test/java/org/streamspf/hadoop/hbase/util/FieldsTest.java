package org.streamspf.hadoop.hbase.util;

import org.streamspf.hadoop.util.Fields;
import com.esotericsoftware.reflectasm.FieldAccess;
import com.esotericsoftware.reflectasm.MethodAccess;
import org.streamspf.hadoop.hbase.SampleBean;
import org.streamspf.hadoop.hbase.exceptions.HTableDefException;
import org.junit.Test;

import static org.junit.Assert.*;

import java.lang.reflect.Field;

public class FieldsTest {
    @Test
    public void testAll() {
        SampleBean bean = new SampleBean();
        Field f = null;
        try {
            f = Fields.getDeclaredField(SampleBean.class, "name");
        } catch (HTableDefException localHTableDefException) {
        }
        try {
            assertEquals(SampleBean.class.getDeclaredField("name"), f);
        } catch (SecurityException localSecurityException) {
        } catch (NoSuchFieldException localNoSuchFieldException) {
        }
        bean.setName("aaa");
        assertEquals("aaa",
                Fields.getFieldValue(MethodAccess.get(SampleBean.class), FieldAccess.get(SampleBean.class), bean, f));
        Fields.setFieldValue(MethodAccess.get(SampleBean.class), FieldAccess.get(SampleBean.class), bean, f, "else");
        assertEquals("else", bean.getName());
    }
}
