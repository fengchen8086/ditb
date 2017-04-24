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
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by winter on 16-12-21.
 */
public class IRScannerInParallel extends BaseIndexScanner {

  private final int MAX_SCANNER_SIZE = 3;
  ArrayList<ResultScanner> scannerList = new ArrayList<>();
  private int scannerIndex = 0;
  Queue<HRegionLocation> regionLocationQueue;
  final int INIT_REGION_SIZE;
  Table table;
  IOException abortException = null;
  AtomicInteger runningGet = new AtomicInteger(0);
  long MAX_WAIT_TIME = 60 * 1000;

  public IRScannerInParallel(Connection conn, IndexTableRelation relation, Scan scan) throws IOException {
    super(conn, relation, scan);
    table = conn.getTable(relation.getTableName());
    RegionLocator locator = conn.getRegionLocator(relation.getTableName());
    regionLocationQueue = new LinkedList<>(locator.getAllRegionLocations());
    INIT_REGION_SIZE = regionLocationQueue.size();
    addNewScanner(true);
    for (int i = 1; i < MAX_SCANNER_SIZE; ++i) {
      addNewScanner(false);
    }
  }

  private int getRegionNumber() throws IOException {
    RegionLocator locator = conn.getRegionLocator(relation.getTableName());
    return locator.getAllRegionLocations().size();
  }

  private void waitUntilDone() {
    long start = System.currentTimeMillis();
    while (runningGet.get() != 0 && (System.currentTimeMillis() - start <= MAX_WAIT_TIME)) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  @Override public Result next() throws IOException {
    if (abortException != null) throw abortException;
    if (scannerList.isEmpty()) return null;
    Result res = null;
    long timeStart = System.currentTimeMillis();
    while (res == null) {
      res = scannerList.get(scannerIndex).next();
      if (res != null) {
        scannerIndex = (scannerIndex + 1) % scannerList.size();
      } else {
        ResultScanner scanner = scannerList.remove(scannerIndex);
        IOUtils.closeQuietly(scanner);
        if (scannerIndex >= scannerList.size()) {
          scannerIndex = 0;
        }
        addNewScanner(scannerList.isEmpty());
        if (scannerList.isEmpty()) {
          waitUntilDone();
          if (scannerList.isEmpty()) {
            break;
          }
        }
      }
    }
    totalScanTime += (System.currentTimeMillis() - timeStart);
    ++totalNumberOfRecords;
    return res;
  }

  @Override public Result[] next(int nbRows) throws IOException {
    Result[] results = new Result[nbRows];
    for (int i = 0; i < nbRows; i++) {
      results[i] = next();
      if (results[i] == null) {
        Result[] newRes = new Result[i];
        System.arraycopy(results, 0, newRes, 0, i);
        return newRes;
      }
    }
    return results;
  }

  @Override public void printScanLatencyStatistic() {
    System.out.println(String.format(
        "IRScanner, cost %d time to scan %d records for scan %d, average latency %.2f seconds",
        totalScanTime, totalNumberOfRecords, rawScan.getId(),
        totalNumberOfRecords / (totalScanTime / 1000.0)));
  }

  @Override public void close() {
    waitUntilDone();
    printScanLatencyStatistic();
    IOUtils.closeQuietly(table);
  }

  private void addNewScanner(boolean isSync) {
    HRegionLocation regionLocation = null;
    synchronized (regionLocationQueue) {
      regionLocation = regionLocationQueue.poll();
    }
    if (regionLocation == null) return;
    runningGet.incrementAndGet();
    if (!isSync) {
      new Thread(new GetScannerRunnable(regionLocation)).start();
      return;
    }
    try {
      innerAddScanner(regionLocation);
    } catch (IOException e) {
      e.printStackTrace();
      abortException = e;
    }
  }

  private void innerAddScanner(HRegionLocation regionLocation) throws IOException {
    if (INIT_REGION_SIZE != getRegionNumber()) {
      throw new IOException(
          "region number changed from " + INIT_REGION_SIZE + " to " + getRegionNumber());
    }
    Scan newScan = new Scan(rawScan);
    if (regionLocation.getRegionInfo().getStartKey() != null)
      newScan.setStartRow(regionLocation.getRegionInfo().getStartKey());
    if (regionLocation.getRegionInfo().getEndKey() != null)
      newScan.setStopRow(regionLocation.getRegionInfo().getEndKey());
    newScan.setAttribute(IndexConstants.SCAN_WITH_INDEX, Bytes.toBytes("Hi"));
    newScan.setFilter(rangeList.toFilterList());
    newScan.setAttribute(IndexConstants.MAX_SCAN_SCALE, Bytes.toBytes(1.0f));
    ResultScanner scanner = table.getScanner(newScan);
    synchronized (scannerList) {
      scannerList.add(scanner);
    }
    runningGet.decrementAndGet();
  }

  private class GetScannerRunnable implements Runnable {

    HRegionLocation regionLocation;

    public GetScannerRunnable(HRegionLocation regionLocation) {
      this.regionLocation = regionLocation;
    }

    @Override public void run() {
      try {
        innerAddScanner(regionLocation);
      } catch (IOException e) {
        e.printStackTrace();
        abortException = e;
      }
    }
  }
}
