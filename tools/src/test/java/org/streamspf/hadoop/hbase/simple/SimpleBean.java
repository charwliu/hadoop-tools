package org.streamspf.hadoop.hbase.simple;


import org.streamspf.hadoop.hbase.annotations.HBaseTable;
import org.streamspf.hadoop.hbase.annotations.HColumn;
import org.streamspf.hadoop.hbase.annotations.HRowkey;
import org.streamspf.hadoop.hbase.annotations.HashType;

@HBaseTable(name = "test_simple", autoCreate = true)
public class SimpleBean {

    @HRowkey
    private String rowkey;

    @HColumn(key = "a")
    private int age;

    @HColumn
    private String name;

    @HColumn
    private boolean adult;

    public int hashCode() {
        int result = 1;
        result = 31 * result + (this.adult ? 1231 : 1237);
        result = 31 * result + this.age;
        result = 31 * result + (this.name == null ? 0 : this.name.hashCode());
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
        SimpleBean other = (SimpleBean) obj;
        if (this.adult != other.adult) {
            return false;
        }
        if (this.age != other.age) {
            return false;
        }
        if (this.name == null) {
            if (other.name != null) {
                return false;
            }
        } else if (!this.name.equals(other.name)) {
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

    public int getAge() {
        return this.age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isAdult() {
        return this.adult;
    }

    public void setAdult(boolean adult) {
        this.adult = adult;
    }
}
