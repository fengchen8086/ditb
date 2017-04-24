package org.apache.hadoop.hbase.regionserver.index.lmdindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.index.IndexType;

/**
 * Created by winter on 17-2-28.
 */
public class LMDIndexParameters {

  private boolean useHistory;
  private int threshold;

  public LMDIndexParameters(Configuration conf, int threshold) {
    this.threshold = threshold;
    this.useHistory = conf.getBoolean(LMDIndexConstants.USE_HISTORICAL_BUCKET_MAP, true);
    System.out
        .println("LMDIndex, use history=" + useHistory + ", bucket size threshold=" + threshold);
  }

  public boolean useHistoricalBucketMap() {
    return useHistory;
  }

  public int getThreshold() {
    return threshold;
  }

  public static Path getTmpSecondaryFilePath(Path rawDataPath) {
    return new Path(rawDataPath.getParent(),
        rawDataPath.getName() + LMDIndexConstants.DATA_FILE_SUFFIX);
  }

  public static Path getTmpBucketFilePath(Path rawDataPath) {
    return new Path(rawDataPath.getParent(),
        rawDataPath.getName() + LMDIndexConstants.BUCKET_FILE_SUFFIX);
  }
}
