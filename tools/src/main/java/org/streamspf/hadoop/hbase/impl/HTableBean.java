package org.streamspf.hadoop.hbase.impl;

import org.streamspf.hadoop.hbase.annotations.HRowkeyPart;
import org.streamspf.hadoop.hbase.annotations.HDynamic;
import org.streamspf.hadoop.hbase.annotations.HBaseTable;
import org.streamspf.hadoop.hbase.annotations.HashType;
import org.streamspf.hadoop.hbase.annotations.HColumn;
import com.esotericsoftware.reflectasm.FieldAccess;
import com.esotericsoftware.reflectasm.MethodAccess;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.streamspf.hadoop.common.MD5;
import org.streamspf.hadoop.hbase.exceptions.HBaseDaoException;
import org.streamspf.hadoop.hbase.exceptions.HTableDefException;
import org.streamspf.hadoop.util.Fields;
import org.streamspf.hadoop.util.Hex;
import org.streamspf.hadoop.util.Types;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;

public class HTableBean {

	private Class<?> beanClass;
	private Field hRowkeyField;
	private Field hParentField;
	private MethodAccess methodAccess;
	private FieldAccess fieldAccess;
	private ArrayList<Field> hRelateToFields = Lists.newArrayList();
	private ArrayList<Field> hRowkeyPartFields = Lists.newArrayList();
	private int knownRowkeyPartsBytesLen = 0;
	private int unkownPartsBytesLen = 0;
	private ArrayList<Field> hColumnFields = Lists.newArrayList();
	private ArrayList<Field> hDynamicFields = Lists.newArrayList();
	private Set<String> families = Sets.newHashSet();
	private Set<byte[]> bfamilies = Sets.newHashSet();

	public int getKnownRowkeyPartsBytesLen() {
		return this.knownRowkeyPartsBytesLen;
	}

	public int getUnkownRowkeyPartsBytesLen() {
		return this.unkownPartsBytesLen;
	}

	public void afterPropertiesSet() throws HTableDefException {
		for (String f : this.families) {
			this.bfamilies.add(Bytes.toBytes(f));
		}

		this.knownRowkeyPartsBytesLen = 0;
		this.unkownPartsBytesLen = 0;
		for (Field hRowkeyPartField : this.hRowkeyPartFields) {
			HRowkeyPart hRowkeyPart = hRowkeyPartField.getAnnotation(HRowkeyPart.class);
			int bytesLen = hRowkeyPart.bytesLen();
			if (hRowkeyPart.type() == HashType.HASH) {
                bytesLen = 16;
            } else if (bytesLen <= 0) {
				bytesLen = Types.getBytesLen(hRowkeyPartField.getType());
			}
			if (bytesLen <= 0) {
				this.unkownPartsBytesLen += 1;
			} else {
				this.knownRowkeyPartsBytesLen += bytesLen;
			}
		}

		if (this.unkownPartsBytesLen > 1) {
			throw new HTableDefException("More than one @RowkeyPart has not defined fixed bytesLen");
		}
	}

	public byte[] getRowkey(Object bean) {
		if ((bean instanceof byte[])) {
			return (byte[]) bean;
		}

		if (this.hRowkeyField != null) {
            Object rowkeyValue = bean;
            if (bean.getClass() == this.beanClass) {
                rowkeyValue = Fields.getFieldValue(this.methodAccess, this.fieldAccess, bean, getHRowkeyField());
            }
			return Types.toBytes(rowkeyValue);
		}

		byte[] rowkeyBytes = null;
		for (Field hRowkeyPartField : this.hRowkeyPartFields) {

			Object rowkeyPartValue = Fields.getFieldValue(this.methodAccess, this.fieldAccess, bean, hRowkeyPartField);
            byte[] bytes = Types.toBytes(rowkeyPartValue);

            HRowkeyPart hRowkeyPart = hRowkeyPartField.getAnnotation(HRowkeyPart.class);
            if (hRowkeyPart != null && hRowkeyPart.type() == HashType.HASH) {
                MD5 md5 = new MD5();
                md5.update(bytes);
                bytes = md5.digest();
            }
            rowkeyBytes = rowkeyBytes == null ? bytes : Bytes.add(rowkeyBytes, bytes);
		}

		return rowkeyBytes;
	}

	public void setRowkey(Object bean, byte[] bRowkey) throws HBaseDaoException {
		if (this.hRowkeyField != null) {
			Fields.setFieldValue(this.methodAccess, this.fieldAccess, bean, this.hRowkeyField,
					Types.fromBytes(bRowkey, this.hRowkeyField.getType()));
			return;
		}

		int offset = 0;
		for (Field hRowkeyPartField : this.hRowkeyPartFields) {
			HRowkeyPart hRowkeyPart = hRowkeyPartField.getAnnotation(HRowkeyPart.class);
			int bytesLen = hRowkeyPart.bytesLen();
            if (hRowkeyPart.type() == HashType.HASH) {
                bytesLen = 16;
            } else if (bytesLen <= 0) {
				bytesLen = Types.getBytesLen(hRowkeyPartField.getType());
			}
			if (bytesLen <= 0) {
				bytesLen = bRowkey.length - this.knownRowkeyPartsBytesLen;
			}
			if (bytesLen <= 0) {
				throw new HBaseDaoException("rowkey bytes cannot converted to @RowkeyPart fields' value");
			}

			byte[] rowkeyPartBytes = new byte[bytesLen];
			System.arraycopy(bRowkey, offset, rowkeyPartBytes, 0, bytesLen);
			offset += bytesLen;
			Object rowkeyPartValue = hRowkeyPart.type() == HashType.RAW ? Types.fromBytes(rowkeyPartBytes, hRowkeyPartField.getType()) :
                    "^MD5"+Hex.toHex(rowkeyPartBytes);
			Fields.setFieldValue(this.methodAccess, this.fieldAccess, bean, hRowkeyPartField, rowkeyPartValue);
		}
	}

	public void setHBaseTable(Class<?> beanClass) {
		setBeanClass(beanClass);
		HBaseTable hbaseTable = beanClass.getAnnotation(HBaseTable.class);
		if (hbaseTable.families() != null) {
			this.families.addAll(Arrays.asList(hbaseTable.families()));
		}
	}

	public void setHRowkey(Field field) {
		this.hRowkeyField = field;
	}

	public void addHColumn(Field field) {
		this.hColumnFields.add(field);
		HColumn hcolumn = field.getAnnotation(HColumn.class);
		if (StringUtils.isNotEmpty(hcolumn.family())) {
			this.families.add(hcolumn.family());
		}
	}

	public void addHRowkeyPart(Field field) {
		this.hRowkeyPartFields.add(field);
	}

	public void addHRelateTo(Field field) {
		this.hRelateToFields.add(field);
	}

	public Field getHRowkeyField() {
		return this.hRowkeyField;
	}

	public void setRowkeyField(Field rowkeyField) {
		this.hRowkeyField = rowkeyField;
	}

	public void addHDynamic(Field field) {
		this.hDynamicFields.add(field);
		HDynamic hdynamic = field.getAnnotation(HDynamic.class);
		if (StringUtils.isNotEmpty(hdynamic.family())) {
			this.families.add(hdynamic.family());
		}
	}

	public Class<?> getBeanClass() {
		return this.beanClass;
	}

	public void setBeanClass(Class<?> beanClass) {
		this.beanClass = beanClass;
		this.methodAccess = MethodAccess.get(beanClass);
		this.fieldAccess = FieldAccess.get(beanClass);
	}

	public ArrayList<Field> getHColumnFields() {
		return this.hColumnFields;
	}

	public ArrayList<Field> getHDynamicFields() {
		return this.hDynamicFields;
	}

	public Set<String> getFamilies() {
		return this.families;
	}

	public void setFamilies(Set<String> families) {
		this.families = families;
	}

	public Set<byte[]> getBfamilies() {
		return this.bfamilies;
	}

	public void setBfamilies(Set<byte[]> bfamilies) {
		this.bfamilies = bfamilies;
	}

	public ArrayList<Field> getHRowkeyPartFields() {
		return this.hRowkeyPartFields;
	}

	public ArrayList<Field> getHRelateToFields() {
		return this.hRelateToFields;
	}

	public void setHRelateToFields(ArrayList<Field> hrelateFields) {
		this.hRelateToFields = hrelateFields;
	}

	public Field getHParentField() {
		return this.hParentField;
	}

	public void setHParent(Field field) {
		this.hParentField = field;
	}

	public HBaseTable getHBaseTable() {
		return this.beanClass.getAnnotation(HBaseTable.class);
	}

	public MethodAccess getMethodAccess() {
		return this.methodAccess;
	}

	public FieldAccess getFieldAccess() {
		return this.fieldAccess;
	}
}
