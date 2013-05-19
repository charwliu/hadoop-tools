package org.streamspf.hadoop.hbase.cascade;


import org.streamspf.hadoop.hbase.annotations.HBaseTable;
import org.streamspf.hadoop.hbase.annotations.HColumn;
import org.streamspf.hadoop.hbase.annotations.HRowkey;

@HBaseTable(name = "other", autoCreate = true)
public class Other {

    @HRowkey
    private String id;

    @HColumn
    private String color;

    public int hashCode() {
        int result = 1;
        result = 31 * result + (this.color == null ? 0 : this.color.hashCode());
        result = 31 * result + (this.id == null ? 0 : this.id.hashCode());
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
        Other other = (Other) obj;
        if (this.color == null) {
            if (other.color != null) {
                return false;
            }
        } else if (!this.color.equals(other.color)) {
            return false;
        }
        if (this.id == null) {
            if (other.id != null) {
                return false;
            }
        } else if (!this.id.equals(other.id)) {
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

    public String getColor() {
        return this.color;
    }

    public void setColor(String color) {
        this.color = color;
    }
}
