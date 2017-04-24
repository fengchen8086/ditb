package org.apache.hadoop.hbase.index.scanner;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.index.userdefine.IndexPutParser;
import org.apache.hadoop.hbase.index.userdefine.IndexTableRelation;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Queue;
import java.util.TreeSet;

/**
 * basic global secondary result scanner,
 * 1. scan each index column by secondary index {r1}, {r2}, ...
 * 2. merge {r1}, {r2}... get {r0} holds all possible results
 * 3. scan rawTable on {r0}, filter results by non-index column filters
 */
public class GSScannerCaching extends BaseIndexScanner {

  private static final int LOCAL_CACHE_SIZE = 1;
  private Queue<byte[]> rowkeyQueue;
  private Queue<Result> localCache;
  private Table rawTable;

  public GSScannerCaching(Connection conn, IndexTableRelation relation, Scan scan)
      throws IOException {
    super(conn, relation, scan);
    rowkeyQueue = createRowkeyQueueBySecondaryIndex(conn, relation, scan.getFamilyMap(), rangeList);
    if (rowkeyQueue != null) {
      rawTable = conn.getTable(relation.getTableName());
      localCache = new LinkedList<>();
    } else {
      rawTable = null;
      localCache = null;
    }
  }

  public GSScannerCaching(BaseIndexScanner base, Queue<byte[]> rowkeyQueue) throws IOException {
    super(base);
    this.rowkeyQueue = rowkeyQueue;
    if (rowkeyQueue != null) {
      rawTable = conn.getTable(relation.getTableName());
      localCache = new LinkedList<>();
    } else {
      rawTable = null;
      localCache = null;
    }
  }

  /**
   * scan all index tables, common rowkeys will be saved in rowkeySet
   * can be optimized in 2 ways:
   * 1. scan index tables by the order of #CandidateRowkeys, similar to CCIndex
   * 2. scan index tables in parallel
   *
   * @throws IOException
   */
  public static Queue<byte[]> createRowkeyQueueBySecondaryIndex(Connection conn,
      IndexTableRelation relation, Map<byte[], NavigableSet<byte[]>> familyMap,
      ScanRange.ScanRangeList rangeList) throws IOException {
    TreeSet<byte[]> rowkeySet = null;
    for (ScanRange range : rangeList.getRanges()) {
      Scan scan = new Scan();
      scan.setStartRow(range.getStart());
      scan.setStopRow(range.getStop());
      scan.setFamilyMap(familyMap);
      if (range.getStartTs() != -1 && range.getStopTs() != -1) {
        scan.setTimeRange(range.getStartTs(), range.getStopTs());
      }
      TableName tableName = relation.getIndexTableName(range.getFamily(), range.getQualifier());
      Table table = conn.getTable(tableName);
      ResultScanner scanner = table.getScanner(scan);
      Result res;
      TreeSet<byte[]> candidateSet = new TreeSet<>(Bytes.BYTES_COMPARATOR);
      while ((res = scanner.next()) != null) {
        candidateSet.add(IndexPutParser.parseIndexRowKey(res.getRow())[0]);
      }
      System.out.println("get " + candidateSet.size() + " candidate rowkeys from " + range);
      if (rowkeySet == null) {
        rowkeySet = candidateSet;
      } else {
        rowkeySet = getCommonSet(rowkeySet, candidateSet);
      }
      System.out.println("common key set size " + rowkeySet.size() + " after " + range);
      if (rowkeySet.isEmpty()) { // no commons keys at all, can ignore the rest index tables
        break;
      }
    }
    if (rowkeySet != null && !rowkeySet.isEmpty()) {
      Queue<byte[]> rowkeyQueue = new LinkedList<>();
      for (byte[] rowkey : rowkeySet)
        rowkeyQueue.add(rowkey);
      return rowkeyQueue;
    }
    return null;
  }

  /**
   * get common keys from two sets
   *
   * @param larger
   * @param smaller
   * @return
   */
  private static TreeSet<byte[]> getCommonSet(TreeSet<byte[]> larger, TreeSet<byte[]> smaller) {
    if (larger.size() < smaller.size()) return getCommonSet(smaller, larger);
    TreeSet<byte[]> commonSet = new TreeSet<>(Bytes.BYTES_COMPARATOR);
    for (byte[] key : smaller) {
      if (larger.contains(key)) {
        commonSet.add(key);
      }
    }
    return commonSet;
  }

  @Override public Result next() throws IOException {
    if (rawTable == null) return null;
    if (localCache.isEmpty()) {
      // load cache by batch get
      int size = Math.min(rowkeyQueue.size(), LOCAL_CACHE_SIZE);
      List<Get> gets = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        gets.add(new Get(rowkeyQueue.poll()));
      }
      Result[] results = rawTable.get(gets);
      for (Result res : results) {
        localCache.add(res);
      }
    }
    if (localCache.isEmpty()) {
      // still empty, no more result, set rawTable to null
      rawTable.close();
      rawTable = null;
      return null;
    }
    return localCache.poll();
  }

  @Override public Result[] next(int nbRows) throws IOException {
    if (rawTable == null) return null;
    int resultSize = Math.min(nbRows, localCache.size() + rowkeyQueue.size());
    Result[] results = new Result[resultSize];
    int newGetSize = resultSize - localCache.size();
    int fromCacheSize = Math.min(nbRows, localCache.size());
    int offset;
    for (offset = 0; offset < fromCacheSize; offset++) {
      results[offset] = localCache.poll();
    }
    if (newGetSize > 0) {
      List<Get> gets = new ArrayList<>(newGetSize);
      for (int i = 0; i < newGetSize; i++) {
        gets.add(new Get(rowkeyQueue.poll()));
      }
      Result[] temp = rawTable.get(gets);
      for (int i = 0; i < temp.length; i++) {
        results[i + offset] = temp[i];
      }
    }
    if (rowkeyQueue.isEmpty() && localCache.isEmpty()) {
      // no more results
      rawTable = null;
    }
    return results;
  }

  @Override public void printScanLatencyStatistic() {
    System.out.println(String.format("GSScannerCaching, no statistic at all"));
  }

  @Override public void close() {
    try {
      localCache = null;
      rowkeyQueue = null;
      printScanLatencyStatistic();
      if (rawTable != null) {
        rawTable.close();
        rawTable = null;
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
