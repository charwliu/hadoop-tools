package org.streamspf.hadoop.hbase;

public interface BytesConvertable<T> {

	public byte[] toBytes(T paramT);

	public T fromBytes(byte[] paramArrayOfByte);
}
