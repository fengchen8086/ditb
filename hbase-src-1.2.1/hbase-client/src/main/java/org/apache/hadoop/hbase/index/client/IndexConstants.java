package org.apache.hadoop.hbase.index.client;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Constants used in classed which are related to index.
 */
public final class IndexConstants {

  public static final String REGION_INDEX_DIR_NAME = ".index";
  public static final String SCAN_WITH_INDEX = "scan.with.index";
  public static final String SCAN_INDEX_RANGES = "scan.index.ranges";
  public static final String MAX_SCAN_SCALE = "max.scan.scale";
  public static final float DEFAULT_MAX_SCAN_SCALE = 0.5f;
  public static final String INDEXFILE_REPLICATION = "indexfile.replication";
  public static final int DEFAULT_INDEXFILE_REPLICATION = 3;
  public static final String HREGION_INDEX_COMPACT_CACHE_MAXSIZE =
      "hregion.index.compact.cache.maxsize";
  public static final long DEFAULT_HREGION_INDEX_COMPACT_CACHE_MAXSIZE = 67108864L;
  public static final String INDEX_COMPACTION_LOCAL_DIR = "index.compaction.local.dir";

  // regard base table as an index table on key(an index column name of base table)
  public static final byte[] KEY = Bytes.toBytes("key");
  // minimum row key
  public static final byte[] MIN_ROW_KEY = { (byte) 0x00 };
  // describes indexes and is stored in main data table description
  public static final byte[] INDEX_KEY = Bytes.toBytes("INDEX_KEY");
  // key generator class name
  public static final byte[] KEYGEN = Bytes.toBytes("KEYGEN");
  // the flag of main data table, stored in main data table description
  public static final byte[] BASE_KEY = Bytes.toBytes("BASE_KEY");
  // TODO
  // index type:ccindex, secondary index and improved secondary index, to be fixed!
  public static final byte[] INDEX_TYPE = Bytes.toBytes("INDEX_TYPE");
  // zero series used to construct the last part of a index key
  public static final String LASTPART_ZERO = "00000000";
  // string length of last part of a index key, the last part is a combination of several '0' with the length of the indexed column value
  public static final int LASTPART_LENGTH = LASTPART_ZERO.length();
  // maximum row key
  public static final byte[] MAX_ROW_KEY = { (byte) 0xff };
  // minimum family name used in SecIndex and ImpSecIndex
  public static final byte[] MIN_FAMILY_NAME = new byte[] { 48 };

  public static String CCT_FIX_STR = "_cct";
  public static byte[] CCT_FIX = Bytes.toBytes(CCT_FIX_STR);
}