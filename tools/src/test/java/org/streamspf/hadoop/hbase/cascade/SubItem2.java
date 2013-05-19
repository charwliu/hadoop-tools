package org.streamspf.hadoop.hbase.cascade;

import org.streamspf.hadoop.hbase.annotations.HBaseTable;
import org.streamspf.hadoop.hbase.annotations.HColumn;
import org.streamspf.hadoop.hbase.annotations.HRowkeyPart;

@HBaseTable(name = "sub_item2", autoCreate = true)
public class SubItem2 {
    @HRowkeyPart
    private String id;

    @HRowkeyPart
    private int seq;

    @HColumn
    private String itemName;

    public int hashCode() {
        int result = 1;
        result = 31 * result + (this.id == null ? 0 : this.id.hashCode());
        result = 31 * result + (this.itemName == null ? 0 : this.itemName.hashCode());
        result = 31 * result + this.seq;
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
        SubItem2 other = (SubItem2) obj;
        if (this.id == null) {
            if (other.id != null) {
                return false;
            }
        } else if (!this.id.equals(other.id)) {
            return false;
        }
        if (this.itemName == null) {
            if (other.itemName != null) {
                return false;
            }
        } else if (!this.itemName.equals(other.itemName)) {
            return false;
        }
        if (this.seq != other.seq) {
            return false;
        }
        return true;
    }

    public String getId() {
        return this.id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getItemName() {
        return this.itemName;
    }

    public void setItemName(String itemName) {
        this.itemName = itemName;
    }

    public int getSeq() {
        return this.seq;
    }

    public void setSeq(int seq) {
        this.seq = seq;
    }

}
