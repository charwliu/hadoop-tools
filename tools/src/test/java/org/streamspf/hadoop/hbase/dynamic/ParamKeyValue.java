package org.streamspf.hadoop.hbase.dynamic;


import org.streamspf.hadoop.hbase.BytesConvertable;
import org.apache.hadoop.hbase.util.Bytes;


public class ParamKeyValue implements BytesConvertable<ParamKeyValue> {

    private long salt;
    private String security;

    public byte[] toBytes(ParamKeyValue object) {
        return Bytes.add(Bytes.toBytes(object.getSalt()), Bytes.toBytes(object.getSecurity()));
    }

    public ParamKeyValue fromBytes(byte[] bytes) {
        this.salt = Bytes.toLong(bytes, 0, 8);
        this.security = Bytes.toString(bytes, 8, bytes.length - 8);
        return this;
    }

    public int hashCode() {
        int result = 1;
        result = 31 * result + (int) (this.salt ^ this.salt >>> 32);
        result = 31 * result + (this.security == null ? 0 : this.security.hashCode());
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
        ParamKeyValue other = (ParamKeyValue) obj;
        if (this.salt != other.salt) {
            return false;
        }
        if (this.security == null) {
            if (other.security != null) {
                return false;
            }
        } else if (!this.security.equals(other.security)) {
            return false;
        }
        return true;
    }

    public String getSecurity() {
        return this.security;
    }

    public void setSecurity(String security) {
        this.security = security;
    }

    public long getSalt() {
        return this.salt;
    }

    public void setSalt(long salt) {
        this.salt = salt;
    }
}
