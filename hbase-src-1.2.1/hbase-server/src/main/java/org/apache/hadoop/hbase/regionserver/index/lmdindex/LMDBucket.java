package org.apache.hadoop.hbase.regionserver.index.lmdindex;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.index.mdhbase.MDBucket;
import org.apache.hadoop.hbase.index.mdhbase.MDRange;
import org.apache.hadoop.hbase.index.mdhbase.MDUtils;
import org.apache.hadoop.hbase.util.Bytes;

import javax.swing.plaf.metal.MetalDesktopIconUI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by winter on 17-3-14.
 */
public class LMDBucket {

  private byte[] startKey;
  private byte[] stopKey;
  private int prefix;
  private int dimensions;
  private List<Cell> cells;

  public LMDBucket(byte[] startKey, int prefix, int dimensions) {
    this.startKey = startKey;
    this.prefix = prefix;
    // mask = 11111100000..
    byte[] mask = MDUtils.makeMask(prefix, dimensions);
    // now mask = 000000111...
    mask = MDUtils.not(mask);
    // end = 000001111111...
    stopKey = MDUtils.or(startKey, mask);
    this.dimensions = dimensions;
  }

  public LMDBucket(byte[] startKey, byte[] stopKey, int prefix, int dimensions) {
    this.startKey = startKey;
    this.stopKey = stopKey;
    this.prefix = prefix;
    this.dimensions = dimensions;
  }

  public void addCell(Cell cell) {
    if (cells == null) cells = new ArrayList<>();
    cells.add(cell);
  }

  public void addCells(List<Cell> newCells) {
    if (newCells == null || newCells.isEmpty()) return;
    if (cells == null) cells = new ArrayList<>();
    cells.addAll(newCells);
  }

  public int cellSize() {
    if (cells == null) return 0;
    return cells.size();
  }

  public byte[] getStartKey() {
    return startKey;
  }

  public byte[] getStopKey() {
    return stopKey;
  }

  public int getPrefix() {
    return prefix;
  }

  public int getDimensions() {
    return dimensions;
  }

  @Override public String toString() {
    return String
        .format("[%s-%s, prefix=%d, range %s with %d records]", MDUtils.toBitString(startKey),
            MDUtils.toBitString(stopKey), prefix, coveredRangesToString(), cellSize());
  }

  private String coveredRangesToString() {
    StringBuilder sb = new StringBuilder();
    sb.append("startBucket with values:[");
    int[] values = MDUtils.bitwiseUnzip(startKey, 3);
    for (int v : values)
      sb.append(v).append(",");
    sb.append("], stopBucket with values:[");
    values = MDUtils.bitwiseUnzip(stopKey, 3);
    for (int v : values)
      sb.append(v).append(",");
    sb.append(")");
    return sb.toString();
  }

  public KeyValue toKeyValue() {
    return new KeyValue(startKey, LMDIndexConstants.FAMILY, LMDIndexConstants.QUALIFIER,
        Bytes.toBytes(prefix));
  }

  public boolean containsKey(byte[] key) {
    return Bytes.compareTo(startKey, key) <= 0 && Bytes.compareTo(key, stopKey) <= 0;
  }

  private boolean isNeighbour(LMDBucket laterOne) {
    if (prefix != laterOne.prefix) return false;
    if (dimensions != laterOne.dimensions) return false;
    byte[] copy = new byte[stopKey.length];
    System.arraycopy(stopKey, 0, copy, 0, stopKey.length);
    byte[] values = MDUtils.increment(copy);
    return Bytes.compareTo(values, laterOne.startKey) == 0;
  }

  public MDRange[] getMDRanges() {
    int[] mins = MDUtils.bitwiseUnzip(startKey, dimensions);
    int[] maxs = MDUtils.bitwiseUnzip(stopKey, dimensions);
    MDRange[] ranges = new MDRange[mins.length];
    for (int i = 0; i < mins.length; i++) {
      ranges[i] = new MDRange(mins[i], maxs[i]);
    }
    return ranges;
  }

  public String cellNames() {
    if (cellSize() <= 0) return "[empty]";
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    for (Cell cell : cells) {
      sb.append(Bytes.toInt(cell.getQualifier())).append(",");
    }
    sb.append(" totally ").append(cellSize()).append(" records }");
    return sb.toString();
  }

  public static List<LMDBucket> createEmptyBuckets(byte[] maxMDKey, int nbBuckets, int dimensions) {
    List<LMDBucket> buckets = new ArrayList<>(nbBuckets + 1);
    // maxMDKey = 000001xxx, mask=6
    int prefix = MDUtils.getPrefix(maxMDKey);
    // mask = 11111100000..
    byte[] mask = MDUtils.makeMask(prefix, dimensions);
    // middle = 0000010000...
    byte[] middle = MDUtils.and(maxMDKey, mask);
    // now mask = 000000111...
    mask = MDUtils.not(mask);
    // end = 000001111111...
    byte[] end = MDUtils.or(maxMDKey, mask);
    byte[] start = new byte[maxMDKey.length];
    Arrays.fill(start, (byte) 0);
    if (nbBuckets <= 2) {
      buckets.add(new LMDBucket(start, prefix, dimensions));
      buckets.add(new LMDBucket(middle, prefix, dimensions));
      //      buckets.add(new LMDBucket(end, prefix, dimensions));
    } else {
      nbBuckets /= 2;
      int bits = 0;
      while (nbBuckets > 0) {
        ++bits;
        nbBuckets /= 2;
      }
      List<byte[]> list = new ArrayList<>(nbBuckets);
      MDUtils.splitBytes(list, start, prefix + bits - 1, prefix + 1);
      MDUtils.splitBytes(list, middle, prefix + bits - 1, prefix + 1);
      for (byte[] key : list)
        buckets.add(new LMDBucket(key, prefix + bits - 1, dimensions));
      //      buckets.add(new LMDBucket(end, prefix + bits - 1, dimensions));
    }
    return buckets;
  }

  public static List<LMDBucket> mergeAndSplitBuckets(List<LMDBucket> buckets, int threshold,
      int dimensions, String desc) {
    List<LMDBucket> retList = new ArrayList<>();
    int splits = 0, merges = 0;
    for (LMDBucket bucket : buckets) {
      if (bucket.cellSize() > threshold) {
        splits += splitBuckets(retList, bucket, threshold, dimensions);
      } else {
        merges += addAndTryMerge(retList, bucket, threshold);
      }
    }
    System.out.println(String.format("%d merges, %d splits in work [%s]", merges, splits, desc));
    return retList;
  }

  public static int addAndTryMerge(List<LMDBucket> buckets, LMDBucket cur, int threshold) {
    if (buckets.isEmpty()) {
      buckets.add(cur);
      return 0;
    }
    LMDBucket prev = buckets.get(buckets.size() - 1);
    if (prev.isNeighbour(cur) && (prev.cellSize() + cur.cellSize() <= threshold)
        && prev.prefix <= LMDIndexConstants.MIN_BUCKET_PREFIX) { // cannot be too small
      // merge the two
      prev.addCells(cur.cells);
      prev.prefix--;
      prev.stopKey = cur.stopKey;
      return 1;
    } else {
      buckets.add(cur);
      return 0;
    }
  }

  public static int splitBuckets(List<LMDBucket> buckets, LMDBucket cur, int threshold,
      int dimensions) {
    if (cur.prefix >= LMDIndexConstants.MAX_BUCKET_PREFIX) {
      buckets.add(cur);
      return 0;
    }
    int splits = 1;
    List<byte[]> keyList = new ArrayList(2);
    MDUtils.splitBytes(keyList, cur.startKey, cur.prefix + 1, cur.prefix + 1);
    LMDBucket zero = new LMDBucket(keyList.get(0), cur.prefix + 1, dimensions);
    LMDBucket one = new LMDBucket(keyList.get(1), cur.prefix + 1, dimensions);
    int splitIndex = 0;
    for (; splitIndex < cur.cellSize(); splitIndex++) {
      if (!zero.containsKey(cur.cells.get(splitIndex).getRow())) break;
    }
    zero.addCells(cur.cells.subList(0, splitIndex));
    one.addCells(cur.cells.subList(splitIndex, cur.cellSize()));
    if (zero.cellSize() > threshold) {
      splits += splitBuckets(buckets, zero, threshold, dimensions);
    } else {
      buckets.add(zero);
    }
    if (one.cellSize() > threshold) {
      splits += splitBuckets(buckets, one, threshold, dimensions);
    } else {
      buckets.add(one);
    }
    return splits;
  }
}
