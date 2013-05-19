package org.streamspf.hadoop.hbase.cascade;

import org.streamspf.hadoop.hbase.annotations.HBaseTable;
import org.streamspf.hadoop.hbase.annotations.HCascade;
import org.streamspf.hadoop.hbase.annotations.HColumn;
import org.streamspf.hadoop.hbase.annotations.HRowkey;

import java.util.List;

@HBaseTable(name = "main_table", autoCreate = true, families = {"f"})
public class MainTable {
    @HRowkey
    private String id;

    @HColumn
    private String name;

    @HCascade
    private List<SubItem> subItems;

    @HCascade
    private List<SubItem2> subItems2;

    @HCascade
    private Other other;

    public int hashCode() {
        int result = 1;
        result = 31 * result + (this.id == null ? 0 : this.id.hashCode());
        result = 31 * result + (this.name == null ? 0 : this.name.hashCode());
        result = 31 * result + (this.other == null ? 0 : this.other.hashCode());
        result = 31 * result + (this.subItems == null ? 0 : this.subItems.hashCode());
        result = 31 * result + (this.subItems2 == null ? 0 : this.subItems2.hashCode());
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
        MainTable other = (MainTable) obj;
        if (this.id == null) {
            if (other.id != null) {
                return false;
            }
        } else if (!this.id.equals(other.id)) {
            return false;
        }
        if (this.name == null) {
            if (other.name != null) {
                return false;
            }
        } else if (!this.name.equals(other.name)) {
            return false;
        }
        if (this.other == null) {
            if (other.other != null) {
                return false;
            }
        } else if (!this.other.equals(other.other)) {
            return false;
        }
        if (this.subItems == null) {
            if (other.subItems != null) {
                return false;
            }
        } else if (!this.subItems.equals(other.subItems)) {
            return false;
        }
        if (this.subItems2 == null) {
            if (other.subItems2 != null) {
                return false;
            }
        } else if (!this.subItems2.equals(other.subItems2)) {
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

    public List<SubItem> getSubItems() {
        return this.subItems;
    }

    public void setSubItems(List<SubItem> subItems) {
        this.subItems = subItems;
    }

    public List<SubItem2> getSubItems2() {
        return this.subItems2;
    }

    public void setSubItems2(List<SubItem2> subItems2) {
        this.subItems2 = subItems2;
    }

    public Other getOther() {
        return this.other;
    }

    public void setOther(Other theOther) {
        this.other = theOther;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
