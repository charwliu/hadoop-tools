package org.streamspf.hadoop.hbase.dynamic;

import org.streamspf.hadoop.hbase.BytesConvertable;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;

public class SignKey implements BytesConvertable<SignKey> {
    private byte[] signKey = Bytes.toBytes("signkey");
    private long eff;
    private long exp;

    public byte[] toBytes(SignKey object) {
        return Bytes.add(this.signKey, Bytes.toBytes(object.getEff()), Bytes.toBytes(object.getExp()));
    }

    public SignKey fromBytes(byte[] bytes) {
        if (bytes.length != this.signKey.length + 8 + 8) {
            return null;
        }

        if (!Bytes.equals(this.signKey, Bytes.head(bytes, this.signKey.length))) {
            return null;
        }

        this.eff = Bytes.toLong(bytes, this.signKey.length, 8);
        this.exp = Bytes.toLong(bytes, this.signKey.length + 8, 8);
        return this;
    }

    public int hashCode() {
        int result = 1;
        result = 31 * result + (int) (this.eff ^ this.eff >>> 32);
        result = 31 * result + (int) (this.exp ^ this.exp >>> 32);
        result = 31 * result + Arrays.hashCode(this.signKey);
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
        SignKey other = (SignKey) obj;
        if (this.eff != other.eff) {
            return false;
        }
        if (this.exp != other.exp) {
            return false;
        }
        if (!Arrays.equals(this.signKey, other.signKey)) {
            return false;
        }
        return true;
    }

    public long getEff() {
        return this.eff;
    }

    public void setEff(long eff) {
        this.eff = eff;
    }

    public long getExp() {
        return this.exp;
    }

    public void setExp(long exp) {
        this.exp = exp;
    }
}
