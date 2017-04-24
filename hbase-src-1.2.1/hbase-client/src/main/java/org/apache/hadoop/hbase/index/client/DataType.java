package org.apache.hadoop.hbase.index.client;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Describe the column data type , include String, Integer, Long, Double, etc.
 */
public enum DataType {
  SHORT, INT, LONG, DOUBLE, STRING, BOOLEAN, UNKNOWN;

  public static byte[] stringToBytes(DataType type, String value) {
    switch (type) {
    case SHORT:
      return Bytes.toBytes(Short.valueOf(value));
    case INT:
      return Bytes.toBytes(Integer.valueOf(value));
    case LONG:
      return Bytes.toBytes(Long.valueOf(value));
    case DOUBLE:
      return Bytes.toBytes(Double.valueOf(value));
    case BOOLEAN:
      return Bytes.toBytes(Boolean.valueOf(value));
    case STRING:
      return Bytes.toBytes(value);
    }
    return null;
  }

  public static String byteToString(DataType type, byte[] value) {
    if (value == null) return "value null";
    if (type == null) return "type null";
    switch (type) {
    case SHORT:
      return String.valueOf(Bytes.toShort(value));
    case INT:
      return String.valueOf(Bytes.toInt(value));
    case LONG:
      return String.valueOf(Bytes.toLong(value));
    case DOUBLE:
      return String.valueOf(Bytes.toDouble(value));
    case BOOLEAN:
      return String.valueOf(Bytes.toBoolean(value));
    case STRING:
      return Bytes.toString(value);
    }
    return null;
  }
}

