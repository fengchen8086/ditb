package org.apache.hadoop.hbase.index.userdefine;

import org.apache.hadoop.hbase.index.client.DataType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by winter on 16-11-16.
 */
public class ColumnInfo implements Writable, Comparable<ColumnInfo> {
  byte[] family;
  byte[] qualifier;
  DataType dataType;
  boolean isIndex;
  int hashCode = 0;

  /**
   * used for writable
   */
  public ColumnInfo() {

  }

  public ColumnInfo(byte[] f, byte[] q) {
    this(f, q, DataType.UNKNOWN, false);
  }

  public ColumnInfo(String f, String q, DataType type, boolean isIndex) {
    this(Bytes.toBytes(f), Bytes.toBytes(q), type, isIndex);
  }

  public ColumnInfo(byte[] f, byte[] q, DataType type, boolean isIndex) {
    family = f;
    qualifier = q;
    dataType = type;
    this.isIndex = isIndex;
    hashCode = calHashCode();
  }

  @Override public void write(DataOutput out) throws IOException {
    WritableUtils.writeCompressedByteArray(out, family);
    WritableUtils.writeCompressedByteArray(out, qualifier);
    WritableUtils.writeString(out, dataType.toString());
    WritableUtils.writeVInt(out, isIndex ? 1 : 0);
  }

  @Override public void readFields(DataInput in) throws IOException {
    family = WritableUtils.readCompressedByteArray(in);
    qualifier = WritableUtils.readCompressedByteArray(in);
    dataType = DataType.valueOf(WritableUtils.readString(in));
    isIndex = WritableUtils.readVInt(in) == 1;
    hashCode = calHashCode();
  }

  public byte[] getQualifier() {
    return qualifier;
  }

  public byte[] getFamily() {
    return family;
  }

  @Override public int compareTo(ColumnInfo o) {
    int ret = Bytes.compareTo(family, o.family);
    if (ret == 0) {
      ret = Bytes.compareTo(qualifier, o.qualifier);
    }
    return ret;
  }

  public boolean isSameColumn(byte[] f, byte[] q) {
    if (Bytes.equals(family, f)) return Bytes.equals(qualifier, q);
    return false;
  }

  public void setIsIndex(boolean isIndex) {
    this.isIndex = isIndex;
  }

  private int calHashCode() {
    int ret = 0;
    for (byte b : family) {
      ret = b ^ (ret >>> 16);
    }
    for (byte b : qualifier) {
      ret = b ^ (ret >>> 16);
    }
    return ret;
  }

  @Override public int hashCode() {
    return hashCode;
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof ColumnInfo)) return false;
    ColumnInfo an = (ColumnInfo) o;
    return hashCode == an.hashCode && isSameColumn(an.family, an.qualifier);
  }

  @Override public String toString() {
    return "[" + Bytes.toString(family) + ":" + Bytes.toString(qualifier) + ", " + dataType + ", "
        + isIndex + "]";
  }

  public static String transToString(byte[] family, byte[] qualifier) {
    return "[" + Bytes.toString(family) + ":" + Bytes.toString(qualifier) + "]";
  }
}
