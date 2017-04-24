package org.apache.hadoop.hbase.regionserver.index.lmdindex;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.index.mdhbase.MDRange;
import org.apache.hadoop.hbase.index.mdhbase.MDUtils;
import org.apache.hadoop.hbase.index.scanner.ScanRange;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.NonLazyKeyValueScanner;
import org.apache.hadoop.hbase.regionserver.ScanQueryMatcher;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Created by winter on 17-2-28.
 */
public class LMDIndexDirectStoreFileScanner extends NonLazyKeyValueScanner {
  private static final Log LOG = LogFactory.getLog(LMDIndexDirectStoreFileScanner.class);
  private StoreFileScanner rawDataScanner;
  private List<byte[]> rawRowkeyList;
  private int rawRowkeyListIndex;
  private Cell cur;
  private boolean canUseDrop;
  private boolean cacheBlocks;
  private boolean usePread;
  private boolean isCompaction;
  private ScanQueryMatcher matcher;
  private long readPt;
  private boolean isPrimaryReplica;
  private StoreFile file;
  private boolean rowkeyAsResult;

  public LMDIndexDirectStoreFileScanner(StoreFile file, boolean canUseDrop, boolean cacheBlocks,
      boolean usePread, boolean isCompaction, ScanQueryMatcher matcher, long readPt,
      boolean isPrimaryReplica, TreeMap<byte[], TreeSet<byte[]>> indexFamilyMap,
      ScanRange.ScanRangeList rangeList, FileSystem fileSystem, CacheConfig cacheConf,
      Configuration conf, boolean rowkeyAsResult) throws IOException {
    this.canUseDrop = canUseDrop;
    this.cacheBlocks = cacheBlocks;
    this.usePread = usePread;
    this.isCompaction = isCompaction;
    this.matcher = matcher;
    this.readPt = readPt;
    this.isPrimaryReplica = isPrimaryReplica;
    this.file = file;
    this.rowkeyAsResult = rowkeyAsResult;
    try {
      rawRowkeyList = initRowKeyList(fileSystem, cacheConf, conf, indexFamilyMap, rangeList);
      System.out.println("get " + rawRowkeyList.size() + " secondary rowkeys from " + this.file
          + ", now rowkeyAsResult=" + rowkeyAsResult);
      if (!rowkeyAsResult) {
        Collections.sort(rawRowkeyList, Bytes.BYTES_COMPARATOR);
        rawDataScanner = getStoreFileScanner(file);
      }
    } catch (IOException e) {
      System.out.println("error in LMDIndexStoreFileScanner, " + e);
      throw e;
    }
  }

  private List<byte[]> initRowKeyList(FileSystem fileSystem, CacheConfig cacheConf,
      Configuration conf, TreeMap<byte[], TreeSet<byte[]>> indexFamilyMap,
      ScanRange.ScanRangeList rangeList) throws IOException {
    // init
    StoreFile bucketStoreFile =
        new StoreFile(fileSystem, LMDIndexParameters.getTmpBucketFilePath(file.getPath()), conf,
            cacheConf, BloomType.NONE);
    StoreFile secondaryStoreFile =
        new StoreFile(fileSystem, LMDIndexParameters.getTmpSecondaryFilePath(file.getPath()), conf,
            cacheConf, BloomType.NONE);
    StoreFileScanner bucketScanner = getStoreFileScanner(bucketStoreFile);
    StoreFileScanner secondaryScanner = getStoreFileScanner(secondaryStoreFile);
    // get hit buckets
    MDRange[] ranges = getRanges(indexFamilyMap, rangeList);
    List<LMDBucket> bucketList = getBucketRanges(bucketScanner, ranges);
    // scan rowkeys based on the buckets
    List<byte[]> rowkeyList = getRawRowkeyList(secondaryScanner, bucketList, ranges);
    // deinit
    bucketScanner.close();
    bucketStoreFile.closeReader(true);
    secondaryScanner.close();
    secondaryStoreFile.closeReader(true);
    return rowkeyList;
  }

  private StoreFileScanner getStoreFileScanner(StoreFile storeFile) throws IOException {
    StoreFile.Reader r = storeFile.createReader(canUseDrop);
    r.setReplicaStoreFile(isPrimaryReplica);
    StoreFileScanner scanner = r.getStoreFileScanner(cacheBlocks, usePread, isCompaction, readPt);
    scanner.setScanQueryMatcher(matcher);
    return scanner;
  }

  private List<byte[]> getRawRowkeyList(StoreFileScanner secondaryScanner,
      List<LMDBucket> bucketList, MDRange[] ranges) throws IOException {
    List<byte[]> rowkeyList = new ArrayList<>();
    for (LMDBucket bucket : bucketList) {
      Cell peekCell = secondaryScanner.peek();
      if (peekCell != null && Bytes.compareTo(bucket.getStartKey(), peekCell.getRow()) == 0) {
      } else {
        secondaryScanner.reseek(new KeyValue(bucket.getStartKey(), LMDIndexConstants.FAMILY,
            LMDIndexConstants.QUALIFIER));
      }
      Cell cell;
      while ((cell = secondaryScanner.peek()) != null) {
        if (Bytes.compareTo(bucket.getStopKey(), cell.getRow()) < 0) {
          break;
        }
        boolean included = true;
        int[] values = MDUtils.bitwiseUnzip(cell.getRow(), ranges.length);
        for (int i = 0; i < ranges.length; i++) {
          if (!ranges[i].include(values[i])) {
            included = false;
            break;
          }
        }
        if (included) {
          //          System.out.println("adding key: " + Bytes.toInt(cell.getQualifier()));
          rowkeyList.add(cell.getQualifier());
          secondaryScanner.next();
        } else {
          //          System.out.println("skipped key: " + Bytes.toInt(cell.getQualifier()));
          secondaryScanner.reseek(
              new KeyValue(cell.getRow(), LMDIndexConstants.FAMILY, LMDIndexConstants.QUALIFIER));
        }
      }
    }
    return rowkeyList;
  }

  private List<LMDBucket> getBucketRanges(StoreFileScanner bucketScanner, MDRange ranges[])
      throws IOException {
    int[] mins = new int[ranges.length];
    int[] maxs = new int[ranges.length];
    for (int i = 0; i < ranges.length; i++) {
      mins[i] = ranges[i].min;
      maxs[i] = ranges[i].max;
    }
    byte[] probeKey = MDUtils.bitwiseZip(mins, ranges.length);
    KeyValue probeKeyValue =
        new KeyValue(probeKey, LMDIndexConstants.FAMILY, LMDIndexConstants.QUALIFIER);
    if (!bucketScanner.seekToPreviousRow(probeKeyValue)) {
      bucketScanner.seek(KeyValue.LOWESTKEY);
    }
    byte[] stopKey = MDUtils.increment(MDUtils.bitwiseZip(maxs, ranges.length));
    // calculate buckets
    List<LMDBucket> hitBuckets = new ArrayList<>();
    Cell cell;
    while ((cell = bucketScanner.next()) != null) {
      if (Bytes.compareTo(stopKey, cell.getRow()) <= 0) {
        break;
      }
      LMDBucket bucket = new LMDBucket(cell.getRow(), Bytes.toInt(cell.getValue()), ranges.length);
      MDRange[] rs = bucket.getMDRanges();
      boolean intersect = MDRange.intersect(ranges, rs);
      if (intersect) {
        hitBuckets.add(bucket);
      }
    }
    // merge buckets!
    List<LMDBucket> finalBuckets = new ArrayList<>();
    for (LMDBucket bucket : hitBuckets) {
      LMDBucket.addAndTryMerge(finalBuckets, bucket, Integer.MAX_VALUE);
    }
    return finalBuckets;
  }

  private MDRange[] getRanges(TreeMap<byte[], TreeSet<byte[]>> indexFamilyMap,
      ScanRange.ScanRangeList rangeList) {
    List<MDRange> mdRangeList = new ArrayList<>();
    for (Map.Entry<byte[], TreeSet<byte[]>> entry : indexFamilyMap.entrySet()) {
      for (byte[] qualifier : entry.getValue()) {
        boolean match = false;
        for (ScanRange range : rangeList.getRanges()) {
          if (Bytes.compareTo(entry.getKey(), range.getFamily()) == 0
              && Bytes.compareTo(qualifier, range.getQualifier()) == 0) {
            int min = (range.getStart() == null) ? 0 : Bytes.toInt(range.getStart());
            int max =
                range.getRawStop() == null ? Integer.MAX_VALUE : Bytes.toInt(range.getRawStop());
            mdRangeList.add(new MDRange(min, max));
            match = true;
          }
        }
        if (!match) {
          mdRangeList.add(new MDRange(0, Integer.MAX_VALUE));
        }
      }
    }
    return mdRangeList.toArray(new MDRange[mdRangeList.size()]);
  }

  public List<byte[]> getRowkeyList() throws IOException {
    if (!rowkeyAsResult) throw new IOException("should not be called when rowkeyAsResult is false");
    return rawRowkeyList;
  }

  @Override public Cell peek() {
    return cur;
  }

  @Override public Cell next() throws IOException {
    Cell retKey = cur;
    // only seek if we aren't at the end. cur == null implies 'end'.
    if (cur != null) {
      // firstly, check columns in the same row
      rawDataScanner.next();
      Cell peek = rawDataScanner.peek();
      if (peek == null) { // end of file
        setCurrentCell(null);
      } else if (Bytes.compareTo(peek.getRow(), retKey.getRow()) == 0) { // columns in the same row
        setCurrentCell(peek);
      } else if (rawRowkeyListIndex >= rawRowkeyList.size()) { // next row, but no more keys
        setCurrentCell(null);
      } else { // more keys
        rawDataScanner.reseek(new KeyValue(rawRowkeyList.get(rawRowkeyListIndex++), null, null));
        setCurrentCell(rawDataScanner.peek());
      }
    }
    return retKey;
  }

  @Override public boolean seek(Cell key) throws IOException {
    for (int i = rawRowkeyListIndex; i < rawRowkeyList.size(); i++) {
      int cmp = KeyValue.COMPARATOR.compare(key, new KeyValue(rawRowkeyList.get(i), null, null));
      if (cmp <= 0) {
        rawRowkeyListIndex = i;
        rawDataScanner.seek(new KeyValue(rawRowkeyList.get(rawRowkeyListIndex++), null, null));
        setCurrentCell(rawDataScanner.peek());
        return true;
      }
    }
    System.out.println("return false at rawRowkeyListIndex=" + rawRowkeyListIndex);
    setCurrentCell(null);
    return false;
  }

  @Override public boolean reseek(Cell key) throws IOException {
    throw new IOException("not implemented in " + this.getClass().getName());
  }

  @Override public long getSequenceID() {
    return rawDataScanner.getSequenceID();
  }

  @Override public void close() {
    rawDataScanner.close();
    rawRowkeyList = null;
  }

  @Override public boolean shouldUseScanner(Scan scan, Store store, long oldestUnexpiredTS) {
    return true;
  }

  public void dowhat() throws IOException {
    if (rawRowkeyListIndex < rawRowkeyList.size()) {
      rawDataScanner.seek(new KeyValue(rawRowkeyList.get(rawRowkeyListIndex), null, null));
      setCurrentCell(rawDataScanner.peek());
    } else {
      setCurrentCell(null);
    }
  }

  @Override public boolean isFileScanner() {
    return true;
  }

  @Override public boolean backwardSeek(Cell key) throws IOException {
    throw new IOException("not implemented in " + this.getClass().getName());
  }

  @Override public boolean seekToPreviousRow(Cell key) throws IOException {
    throw new IOException("not implemented in " + this.getClass().getName());
  }

  @Override public boolean seekToLastRow() throws IOException {
    throw new IOException("not implemented in " + this.getClass().getName());
  }

  @Override public Cell getNextIndexedKey() {
    return null; // same as memstore
  }

  protected void setCurrentCell(Cell newVal) throws IOException {
    this.cur = newVal;
  }

  public static List<KeyValueScanner> getLMDIndexDirectScannersForStoreFiles(
      Collection<StoreFile> files, boolean cacheBlocks, boolean usePread, boolean isCompaction,
      boolean canUseDrop, ScanQueryMatcher matcher, long readPt, boolean isPrimaryReplica,
      TreeMap<byte[], TreeSet<byte[]>> indexFamilyMap, ScanRange.ScanRangeList rangeList,
      FileSystem fileSystem, CacheConfig cacheConf, Configuration conf, boolean rowkeyAsResult)
      throws IOException {
    List<KeyValueScanner> scanners = new ArrayList<>(files.size());
    for (StoreFile file : files) {
      scanners.add(
          new LMDIndexDirectStoreFileScanner(file, canUseDrop, cacheBlocks, usePread, isCompaction,
              matcher, readPt, isPrimaryReplica, indexFamilyMap, rangeList, fileSystem, cacheConf,
              conf, rowkeyAsResult));
    }
    return scanners;
  }

  private String rowkeyToRanges(byte[] rowkey, int dimensions) {
    if (rowkey == null) return "null";
    int[] values = MDUtils.bitwiseUnzip(rowkey, dimensions);
    StringBuilder sb = new StringBuilder();
    sb.append("rowkey: ").append(Bytes.toString(rowkey)).append(arrayToString(values));
    return sb.toString();
  }

  private String arrayToString(int[] values) {
    if (values == null) return "null";
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    for (int v : values) {
      sb.append(v).append(",");
    }
    sb.append("]");
    return sb.toString();
  }
}
