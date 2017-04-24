package org.apache.hadoop.hbase.index;

import org.apache.hadoop.hbase.util.Bytes;

public enum IndexType {
  LSIndex, // Local and Secondary
  IRIndex, // Local and Secondary, InsideRegion
  LCIndex, // Local and Clustering
  GSIndex, // Global and Secondary
  GCIndex, // Global and Clustering
  CCIndex, // Global and Clustering, Complementary Clustering
  UDLIndex, // User Define Local Index, say local secondary index with additional columns
  UDGIndex, // User Define Global Index, say global secondary index with additional columns
  MDIndex, //
  LMDIndex_S, //
  LMDIndex_D, //
  NoIndex,// No index at all
  // belows are for tests of DITB
  PF_CCT_, PF_CCIT, PF_SCND, PF_MBKT, PF_MSND, PF_BKRW;

  public static final byte[] CCT_SUFFIX_BYTES = Bytes.toBytes("_cct_");
  public static final byte[] SEDONDARY_FAMILY_BYTES = Bytes.toBytes("f");
  public static final String CCIT_REPLICATION_FACTOR_ATTRIBUTE = "_ccit_replication_factor";

  public static boolean isSecondaryIndex(IndexType indexType) {
    return indexType == GSIndex || indexType == LSIndex || indexType == IRIndex;
  }

  public static boolean isClusteringIndex(IndexType indexType) {
    return indexType == GCIndex || indexType == LCIndex || indexType == CCIndex;
  }

  public static boolean isUserDefinedIndex(IndexType indexType) {
    return indexType == UDLIndex || indexType == UDGIndex;
  }

  public static boolean isLocalIndex(IndexType indexType) {
    return indexType == UDLIndex || indexType == LCIndex || indexType == IRIndex
        || indexType == LSIndex || indexType == LMDIndex_S || indexType == LMDIndex_D;
  }

  public static boolean isMDPerf(IndexType indexType) {
    return indexType == PF_MBKT || indexType == PF_MSND || indexType == PF_BKRW;
  }
}
