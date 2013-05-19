package org.streamspf.hadoop.hbase.dynamic;

import org.streamspf.hadoop.hbase.annotations.HColumn;
import org.streamspf.hadoop.hbase.annotations.HTypePair;
import org.streamspf.hadoop.hbase.annotations.HBaseTable;
import org.streamspf.hadoop.hbase.annotations.HRowkey;
import org.streamspf.hadoop.hbase.annotations.HDynamic;

import java.util.Map;

@HBaseTable(name = "dynamicbean", autoCreate = true, families = {"f"})
public class DynamicBean {

    @HRowkey
    private String rowkey;

    @HColumn
    private long appid;

    @HColumn
    private String desc;

    @HDynamic(mapping = {@HTypePair(keyType = SignKey.class, valueType = String.class), @HTypePair(keyType = ParamKey.class, valueType = ParamKeyValue.class), @HTypePair(keyType = String.class, valueType = String.class)})
    private Map<Object, Object> dynamicProperties;

    public int hashCode() {
        int result = 1;
        result = 31 * result + (int) (this.appid ^ this.appid >>> 32);
        result = 31 * result + (this.desc == null ? 0 : this.desc.hashCode());
        result = 31 * result + (this.dynamicProperties == null ? 0 : this.dynamicProperties.hashCode());
        result = 31 * result + (this.rowkey == null ? 0 : this.rowkey.hashCode());
        return result;
    }

    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        DynamicBean other = (DynamicBean) obj;
        if (this.appid != other.appid) {
            return false;
        }
        if (this.desc == null) {
            if (other.desc != null) {
                return false;
            }
        } else if (!this.desc.equals(other.desc)) {
            return false;
        }
        if (this.dynamicProperties == null) {
            if (other.dynamicProperties != null) {
                return false;
            }
        } else if (!this.dynamicProperties.equals(other.dynamicProperties)) {
            return false;
        }
        if (this.rowkey == null) {
            if (other.rowkey != null) {
                return false;
            }
        } else if (!this.rowkey.equals(other.rowkey)) {
            return false;
        }
        return true;
    }

    public String getRowkey() {
        return this.rowkey;
    }

    public void setRowkey(String rowkey) {
        this.rowkey = rowkey;
    }

    public long getAppid() {
        return this.appid;
    }

    public void setAppid(long appid) {
        this.appid = appid;
    }

    public String getDesc() {
        return this.desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public Map<Object, Object> getDynamicProperties() {
        return this.dynamicProperties;
    }

    public void setDynamicProperties(Map<Object, Object> dynamicProperties) {
        this.dynamicProperties = dynamicProperties;
    }
}
