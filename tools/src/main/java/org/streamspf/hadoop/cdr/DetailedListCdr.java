package org.streamspf.hadoop.cdr;


import org.streamspf.hadoop.hbase.annotations.HBaseTable;
import org.streamspf.hadoop.hbase.annotations.HColumn;
import org.streamspf.hadoop.hbase.annotations.HRowkey;

@HBaseTable(name = "DR_DETAIL", autoCreate = false, families = {"cf"})
public class DetailedListCdr {

    @HRowkey
    public String rowKey;

    @HColumn(key = "dr")
    public String value;

    public String getRowKey() {
        return this.rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public String getValue() {
        return this.value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
