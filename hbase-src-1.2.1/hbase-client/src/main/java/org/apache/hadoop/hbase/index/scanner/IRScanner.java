package org.apache.hadoop.hbase.index.scanner;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.index.client.IndexConstants;
import org.apache.hadoop.hbase.index.userdefine.IndexTableRelation;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by winter on 16-12-21.
 */
public class IRScanner extends BaseIndexScanner {

  Queue<HRegionLocation> regionLocationQueue;
  final int INIT_REGION_SIZE;
  Table table;
  ResultScanner currentScanner;

  public IRScanner(Connection conn, IndexTableRelation relation, Scan scan) throws IOException {
    super(conn, relation, scan);
    table = conn.getTable(relation.getTableName());
    RegionLocator locator = conn.getRegionLocator(relation.getTableName());
    regionLocationQueue = new LinkedList<>(locator.getAllRegionLocations());
    INIT_REGION_SIZE = regionLocationQueue.size();
    currentScanner = getNextScanner();
  }

  private int getRegionNumber() throws IOException {
    RegionLocator locator = conn.getRegionLocator(relation.getTableName());
    return locator.getAllRegionLocations().size();
  }

  private ResultScanner getNextScanner() throws IOException {
    if (INIT_REGION_SIZE != getRegionNumber()) {
      throw new IOException(
          "region number changed from " + INIT_REGION_SIZE + " to " + getRegionNumber());
    }
    if (regionLocationQueue.isEmpty()) return null;
    HRegionLocation regionLocation = regionLocationQueue.poll();
    Scan newScan = new Scan(rawScan);
    if (regionLocation.getRegionInfo().getStartKey() != null)
      newScan.setStartRow(regionLocation.getRegionInfo().getStartKey());
    if (regionLocation.getRegionInfo().getEndKey() != null)
      newScan.setStopRow(regionLocation.getRegionInfo().getEndKey());
    newScan.setAttribute(IndexConstants.SCAN_WITH_INDEX, Bytes.toBytes("Hi"));
    newScan.setFilter(rangeList.toFilterList());
    newScan.setAttribute(IndexConstants.MAX_SCAN_SCALE, Bytes.toBytes(1.0f));
    newScan.setId(rawScan.getId());
    newScan.setCacheBlocks(rawScan.getCacheBlocks());
    newScan.setCaching(rawScan.getCaching());
    return table.getScanner(newScan);
  }

  @Override public Result next() throws IOException {
    if (currentScanner == null) return null;
    long timeStart = System.currentTimeMillis();
    Result res = currentScanner.next();
    while (res == null) {
      currentScanner = getNextScanner();
      if (currentScanner == null) return null;
      res = currentScanner.next();
    }
    totalScanTime += (System.currentTimeMillis() - timeStart);
    ++totalNumberOfRecords;
    return res;
  }

  public Result[] innerNext(int nbRows) throws IOException {
    if (currentScanner == null) return null;
    Result[] curResults = currentScanner.next(nbRows);
    Result[] results;
    if (curResults.length == nbRows) {
      results = curResults;
    } else {
      currentScanner = getNextScanner();
      if (currentScanner == null) { // the last region
        results = curResults;
      } else {
        Result[] nextRes = innerNext(nbRows - curResults.length);
        results = new Result[curResults.length + nextRes.length];
        System.arraycopy(curResults, 0, results, 0, curResults.length);
        System.arraycopy(nextRes, 0, results, curResults.length, nextRes.length);
      }
    }
    return results;
  }

  @Override public Result[] next(int nbRows) throws IOException {
    if (currentScanner == null) return null;
    long timeStart = System.currentTimeMillis();
    Result[] results = innerNext(nbRows);
    totalScanTime += (System.currentTimeMillis() - timeStart);
    totalNumberOfRecords += results.length;
    return results;
  }

  @Override public void printScanLatencyStatistic() {
    System.out.println(String.format(
        "IRScanner, cost %.2f time to scan %d records for scan %s, average latency %.2f seconds",
        totalScanTime / 1000.0, totalNumberOfRecords, rawScan.getId(),
        totalNumberOfRecords / (totalScanTime / 1000.0)));
  }

  @Override public void close() {
    printScanLatencyStatistic();
    IOUtils.closeQuietly(table);
  }
}
