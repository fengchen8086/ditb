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

/**
 * Created by winter on 16-12-8.
 */
public class LocalScannerInParallel extends BaseIndexScanner {

  private final int MAX_SCANNER_SIZE = 3;
  ArrayList<ResultScanner> scannerList = new ArrayList<>();
  private int scannerIndex = 0;
  Queue<HRegionLocation> regionLocationQueue;
  Table table;
  IOException abortException = null;

  public LocalScannerInParallel(Connection conn, IndexTableRelation relation, Scan scan) throws IOException {
    super(conn, relation, scan);
    table = conn.getTable(relation.getTableName());
    RegionLocator locator = conn.getRegionLocator(relation.getTableName());
    regionLocationQueue = new LinkedList<>(locator.getAllRegionLocations());
    addNewScanner(true);
    for (int i = 1; i < MAX_SCANNER_SIZE; ++i) {
      addNewScanner(false);
    }
  }

  @Override public Result next() throws IOException {
    if (abortException != null) throw abortException;
    if (scannerList.isEmpty()) return null;
    Result res = null;
    while (res == null && !scannerList.isEmpty()) {
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
      }
    }
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
        "LocalScanner, cost %d time to scan %d records for scan %d, average latency %.2f seconds",
        totalScanTime, totalNumberOfRecords, rawScan.getId(),
        totalNumberOfRecords / (totalScanTime / 1000.0)));
  }

  @Override public void close() {
    printScanLatencyStatistic();
    IOUtils.closeQuietly(table);
  }

  private void addNewScanner(boolean isSync) {
    HRegionLocation regionLocation = null;
    synchronized (regionLocationQueue) {
      regionLocation = regionLocationQueue.poll();
    }
    if (regionLocation == null) return;
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
    Scan newScan = new Scan(rawScan);
    if (regionLocation.getRegionInfo().getStartKey() != null)
      newScan.setStartRow(regionLocation.getRegionInfo().getStartKey());
    if (regionLocation.getRegionInfo().getEndKey() != null)
      newScan.setStopRow(regionLocation.getRegionInfo().getEndKey());
    newScan.setAttribute(IndexConstants.SCAN_WITH_INDEX, Bytes.toBytes("Hi"));
    ResultScanner scanner = table.getScanner(newScan);
    synchronized (scannerList) {
      scannerList.add(scanner);
    }
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
