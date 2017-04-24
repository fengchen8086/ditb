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
import java.util.LinkedList;
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
public class GSScanner extends BaseIndexScanner {

  private Queue<byte[]> rowkeyQueue;
  private Table rawTable;

  public GSScanner(Connection conn, IndexTableRelation relation, Scan scan) throws IOException {
    super(conn, relation, scan);
    rowkeyQueue =
        createRowkeyQueueBySecondaryIndex(conn, relation, scan.getFamilyMap(), rangeList, scan);
    if (rowkeyQueue != null) {
      rawTable = conn.getTable(relation.getTableName());
    } else {
      rawTable = null;
    }
  }

  public GSScanner(BaseIndexScanner base, Queue<byte[]> rowkeyQueue) throws IOException {
    super(base);
    this.rowkeyQueue = rowkeyQueue;
    if (rowkeyQueue != null) {
      rawTable = conn.getTable(relation.getTableName());
    } else {
      rawTable = null;
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
      ScanRange.ScanRangeList rangeList, Scan rawScan) throws IOException {
    TreeSet<byte[]> rowkeySet = null;
    long timeToMerge = 0;
    for (ScanRange range : rangeList.getRanges()) {
      Scan scan = new Scan();
      scan.setStartRow(range.getStart());
      scan.setStopRow(range.getStop());
      scan.setFamilyMap(familyMap);
      scan.setCaching(rawScan.getCaching());
      scan.setCacheBlocks(rawScan.getCacheBlocks());
      scan.setId(rawScan.getId());
      if (range.getStartTs() != -1 && range.getStopTs() != -1) {
        scan.setTimeRange(range.getStartTs(), range.getStopTs());
      }
      TableName tableName = relation.getIndexTableName(range.getFamily(), range.getQualifier());
      Table table = conn.getTable(tableName);
      ResultScanner scanner = table.getScanner(scan);
      Result res;
      long timeStart = System.currentTimeMillis();
      TreeSet<byte[]> candidateSet = new TreeSet<>(Bytes.BYTES_COMPARATOR);
      while ((res = scanner.next()) != null) {
        candidateSet.add(IndexPutParser.parseIndexRowKey(res.getRow())[0]);
      }
      System.out.println(String
          .format("get %d candidate rowkeys from %s in scan %s, cost %.2f seconds",
              candidateSet.size(), range.toString(), scan.getId(),
              (System.currentTimeMillis() - timeStart) / 1000.0));
      if (rowkeySet == null) {
        rowkeySet = candidateSet;
      } else {
        timeStart = System.currentTimeMillis();
        rowkeySet = getCommonSet(rowkeySet, candidateSet);
        timeToMerge += (System.currentTimeMillis() - timeStart);
      }
      System.out.println(
          "common key set size " + rowkeySet.size() + " after " + range + " in scan " + scan
              .getId());
      if (rowkeySet.isEmpty()) { // no commons keys at all, can ignore the rest index tables
        break;
      }
    }
    System.out.println(String
        .format("get %d result rowkeys in scan %s, cost %.2f seconds", rowkeySet.size(),
            rawScan.getId(), timeToMerge / 1000.0));
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
    if (rawTable == null || rowkeyQueue.isEmpty()) return null;
    Get get = new Get(rowkeyQueue.poll());
    long timeStart = System.currentTimeMillis();
    Result result = rawTable.get(get);
    totalScanTime += (System.currentTimeMillis() - timeStart);
    totalNumberOfRecords++;
    return result;
  }

  @Override public Result[] next(int nbRows) throws IOException {
    if (rawTable == null) return null;
    int resultSize = Math.min(nbRows, rowkeyQueue.size());
    Result[] results = new Result[resultSize];
    for (int i = 0; i < resultSize; i++) {
      results[i] = next();
    }
    return results;
  }

  @Override public void printScanLatencyStatistic() {
    System.out.println(String.format(
        "GSScanner, means random get on raw table, cost %.2f time to scan %d records for scan %s, average latency %.2f seconds",
        totalScanTime / 1000.0, totalNumberOfRecords, rawScan.getId(),
        totalNumberOfRecords / (totalScanTime / 1000.0)));
  }

  @Override public void close() {
    try {
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
