package org.streamspf.hadoop.hbase.rowkeypart;


import org.streamspf.hadoop.hbase.annotations.HBaseTable;
import org.streamspf.hadoop.hbase.annotations.HColumn;
import org.streamspf.hadoop.hbase.annotations.HRowkeyPart;
import org.streamspf.hadoop.hbase.annotations.HashType;
import org.apache.commons.lang3.StringUtils;

@HBaseTable(name = "partbean", autoCreate = true)
public class PartBean {
    @HRowkeyPart(type= HashType.HASH)
    private String code;

    @HRowkeyPart
    private int id;

    @HColumn
    private String value;

    public int hashCode() {
        int result = 1;
        result = 31 * result + (this.code == null ? 0 : this.code.hashCode());
        result = 31 * result + this.id;
        result = 31 * result + (this.value == null ? 0 : this.value.hashCode());
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
        PartBean other = (PartBean) obj;
        if (this.code == null) {
            if (other.code != null) {
                return false;
            }
        } else if (!this.code.equals(other.code)) {
            return false;
        }
        if (this.id != other.id) {
            return false;
        }
        if (this.value == null) {
            if (other.value != null) {
                return false;
            }
        } else if (!this.value.equals(other.value)) {
            return false;
        }
        return true;
    }

    public String getCode() {

        if (StringUtils.startsWith(this.code, "^MD5")) {
            return StringUtils.substring(this.code, 4);
        } else {
            return this.code;
        }
    }

    public void setCode(String code) {
        this.code = code;
    }

    public int getId() {
        return this.id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getValue() {
        return this.value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
