package org.apache.hadoop.hbase.index.userdefine;

/**
 * Created by winter on 16-11-28.
 */
public class UDConstants {
  // minimum row key
  public static final byte[] MIN_ROW_KEY = { (byte) 0x00 };
  // zero series used to construct the last part of a index key
  public static final String LASTPART_ZERO = "00000000";
  // string length of last part of a index key, the last part is a combination of several '0' with the length of the indexed column value
  public static final int LASTPART_LENGTH = LASTPART_ZERO.length();
}
