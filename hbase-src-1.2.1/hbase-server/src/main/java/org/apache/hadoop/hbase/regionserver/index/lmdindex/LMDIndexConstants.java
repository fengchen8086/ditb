package org.apache.hadoop.hbase.regionserver.index.lmdindex;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by winter on 17-2-27.
 */
public class LMDIndexConstants {
  public static final String DATA_FILE_SUFFIX = ".lmd_data";
  public static final String BUCKET_FILE_SUFFIX = ".lmd_bucket";
  public static final byte[] FAMILY = Bytes.toBytes("f");
  public static final byte[] QUALIFIER = Bytes.toBytes("q");
  public static final byte[] VALUE = Bytes.toBytes("0");
  public static final int DEFAULT_BUCKET_SIZE_THRESHOLD = 1000;
  public static final int MIN_BUCKET_PREFIX = 2;
  public static final int MAX_BUCKET_PREFIX = 30;

  public static final String USE_HISTORICAL_BUCKET_MAP = "lmdindex.use.historical.bucket.map";
  public static final String BUCKET_THRESHOLD_ATTRIBUTE = "lmdindex.bucket.size.threshold";
}
