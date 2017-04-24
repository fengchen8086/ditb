package org.apache.hadoop.hbase.regionserver.index.lmdindex;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.index.mdhbase.MDBucket;
import org.apache.hadoop.hbase.index.mdhbase.MDUtils;
import org.apache.hadoop.hbase.index.userdefine.IndexTableRelation;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.TimeRangeTracker;
import org.apache.hadoop.hbase.regionserver.index.lcindex.LCIndexConstant;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.TreeSet;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.hadoop.hbase.KeyValue.COMPARATOR;
import static org.apache.hadoop.hbase.KeyValue.Type;
import static org.apache.hadoop.hbase.KeyValue.humanReadableTimestamp;

/**
 * Created by winter on 17-2-27.
 */
public class LMDIndexWriter {
  private static final Log LOG = LogFactory.getLog(LMDIndexWriter.class);
  private final int dimensions;

  private final IndexTableRelation tableRelation;
  private final TimeRangeTracker tracker;
  private final LMDIndexParameters lmdIndexParameters;
  private final Path rawDataPath;
  private final HStore store;
  private final String opType;

  public LMDIndexWriter(HStore store, Path rawDataPath, TimeRangeTracker timeRangeTracker,
      String opType) {
    tableRelation = store.indexTableRelation;
    this.rawDataPath = rawDataPath;
    this.store = store;
    tracker = timeRangeTracker;
    this.opType = opType;
    lmdIndexParameters = store.getLMDIndexParameters();
    int size = 0;
    for (Map.Entry<byte[], TreeSet<byte[]>> entry : tableRelation.getIndexFamilyMap().entrySet()) {
      size += entry.getValue().size();
    }
    dimensions = size;
    int[] mins = new int[dimensions];
    Arrays.fill(mins, 0);
  }

  public void processKeyValueQueue(Queue<KeyValue> kvQueue) throws IOException {
    //    List<LMDBucket>
    byte[] prevRow = null;
    Put put = null;
    // parse kvQueue into putMap
    List<KeyValue> mdRecordList = new ArrayList<>();
    while (!kvQueue.isEmpty()) {
      KeyValue kv = kvQueue.poll();
      byte[] row = CellUtil.cloneRow(kv);
      if (prevRow == null || Bytes.compareTo(prevRow, row) != 0) { // a new put
        processPut(mdRecordList, put);
        put = new Put(row, kv.getTimestamp());
        prevRow = row;
      }
      if (tableRelation.isIndexColumn(CellUtil.cloneFamily(kv), CellUtil.cloneQualifier(kv))) {
        put.add(kv);
      }
    }
    // the last put for mdRecord
    processPut(mdRecordList, put);
    // sort and write secondaries
    Collections.sort(mdRecordList, COMPARATOR);
    // init buckets
    int nbBuckets = getBinaryCelling(mdRecordList.size() / lmdIndexParameters.getThreshold());
    List<LMDBucket> buckets =
        copyBuckets(mdRecordList.get(mdRecordList.size() - 1).getRow(), store.getLMDBuckets(),
            nbBuckets);
    int bucketIndex = 0;
    // write secondary records, update buckets at the same time
    StoreFile.Writer secondaryWriter = createSecondaryWriter(mdRecordList.size());
    for (KeyValue keyValue : mdRecordList) {
      secondaryWriter.append(keyValue);
      //      System.out.println("new data: " + kvToString(keyValue) + " in " + secondaryWriter.getPath());
      boolean added = false;
      for (; bucketIndex < buckets.size(); ++bucketIndex) {
        if (buckets.get(bucketIndex).containsKey(keyValue.getRow())) {
          buckets.get(bucketIndex).addCell(keyValue);
          added = true;
          break;
        }
      }
      checkArgument(added);
    }
    // merge buckets
    buckets = LMDBucket.mergeAndSplitBuckets(buckets, lmdIndexParameters.getThreshold(), dimensions,
        String.format("%d records, %d init buckets in %s", mdRecordList.size(), nbBuckets,
            rawDataPath.toString()));
    // write buckets
    StoreFile.Writer bucketWriter = createBucketWriter(buckets.size());
    for (LMDBucket bucket : buckets) {
      bucketWriter.append(bucket.toKeyValue());
      //      System.out.println("new bucket: " + bucket.toString() + " in " + bucketWriter.getPath());
      //      System.out.println("new bucket: " + bucket.cellNames() + " in " + bucketWriter.getPath());
    }
    LOG.info(String.format(
        "LMDIndex op=%s, raw file=%s, md data file=%s with %d cells, to bucket file=%s with %d buckets",
        opType, rawDataPath, secondaryWriter.getPath(), mdRecordList.size(), bucketWriter.getPath(),
        buckets.size()));
    if (store.getLMDIndexParameters().useHistoricalBucketMap()) {
      store.copyAndSetLMDBuckets(nbBuckets, buckets);
    }
    secondaryWriter.close();
    bucketWriter.close();
  }

  private String kvToString(KeyValue keyValue) {
    StringBuilder sb = new StringBuilder();
    sb.append("rowkey=" + Bytes.toStringBinary(keyValue.getRow()));
    int i = 0;
    int[] arr = MDUtils.bitwiseUnzip(keyValue.getRow(), dimensions);
    sb.append(", indicating=");
    for (Map.Entry<byte[], TreeSet<byte[]>> entry : tableRelation.getIndexFamilyMap().entrySet()) {
      for (byte[] qualifier : entry.getValue()) {
        sb.append("[").append(Bytes.toString(entry.getKey())).append(":")
            .append(Bytes.toString(qualifier)).append("]=").append(arr[i]).append(",");
        ++i;
      }
    }
    sb.append(", rawRowkey=" + Bytes.toInt(keyValue.getQualifier()));
    return sb.toString();
  }

  private StoreFile.Writer createSecondaryWriter(int maxKeyCount) throws IOException {
    Path dataFilePath = lmdIndexParameters.getTmpSecondaryFilePath(rawDataPath);
    StoreFile.Writer writer = store
        .createTmpLMDFileWriter(maxKeyCount, store.family.getCompression(), false, true, false,
            false, dataFilePath);
    if (tracker != null) writer.setTimeRangeTracker(tracker);
    return writer;
  }

  private StoreFile.Writer createBucketWriter(int maxKeyCount) throws IOException {
    Path bucketFilePath = lmdIndexParameters.getTmpBucketFilePath(rawDataPath);
    StoreFile.Writer writer = store
        .createTmpLMDFileWriter(maxKeyCount, store.family.getCompression(), false, true, false,
            false, bucketFilePath);
    if (tracker != null) writer.setTimeRangeTracker(tracker);
    return writer;
  }

  /**
   * parse put, add index put into mdRecordList
   */
  private void processPut(List<KeyValue> mdRecordList, Put put) throws IOException {
    if (put == null) return;
    byte[] rawRowkey = put.getRow();
    int[] arr = new int[dimensions];
    int i = 0;
    for (Map.Entry<byte[], TreeSet<byte[]>> entry : tableRelation.getIndexFamilyMap().entrySet()) {
      for (byte[] qualifier : entry.getValue()) {
        arr[i] = Bytes.toInt(put.get(entry.getKey(), qualifier).get(0).getValue());
        ++i;
      }
    }
    byte[] mdKey = MDUtils.bitwiseZip(arr, dimensions);
    KeyValue keyValue =
        new KeyValue(mdKey, LMDIndexConstants.FAMILY, rawRowkey, put.getTimeStamp(), Type.Put,
            LMDIndexConstants.VALUE);
    mdRecordList.add(keyValue);
  }

  public List<LMDBucket> copyBuckets(byte[] maxMDKey, Map<Integer, List<LMDBucket>> storeBucketMap,
      int nbBuckets) {
    // create empty buckets directly
    if (nbBuckets <= 0) nbBuckets = 1;
    List<LMDBucket> prevBuckets = storeBucketMap.get(nbBuckets);
    if (!store.getLMDIndexParameters().useHistoricalBucketMap() || prevBuckets == null
        || prevBuckets.size() == 0) {
      return LMDBucket.createEmptyBuckets(maxMDKey, nbBuckets, dimensions);
    } else {
      List<LMDBucket> list = new ArrayList<>(prevBuckets.size());
      for (LMDBucket bucket : prevBuckets) {
        list.add(new LMDBucket(bucket.getStartKey(), bucket.getPrefix(), dimensions));
      }
      if (Bytes.compareTo(list.get(list.size() - 1).getStartKey(), maxMDKey) < 0) {
        // maxMDKey not included
        int prefix = MDUtils.getPrefix(maxMDKey);
        byte[] mask = MDUtils.makeMask(prefix, 1);
        byte[] zero = MDUtils.and(maxMDKey, mask);
        list.add(new LMDBucket(zero, prefix, dimensions));
      }
      // check last
      return list;
    }
  }

  public static int getBinaryCelling(int value) {
    int largger = 1;
    boolean perfectNumber = true;
    while (value > 1) {
      if (value % 2 == 1) perfectNumber = false;
      value /= 2;
      largger *= 2;
    }
    if (!perfectNumber) largger *= 2;
    return largger;
  }
}
