package org.apache.hadoop.hbase.index.scanner;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.index.IndexType;
import org.apache.hadoop.hbase.index.userdefine.IndexTableRelation;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Scanner for Global Clustering Scanner, also used in CCIndex
 */
public class GCScanner extends BaseIndexScanner {

  ResultScanner scanner;
  byte[] indexFamily;
  byte[] indexQualifier;

  public GCScanner(Connection conn, IndexTableRelation relation, Scan scan) throws IOException {
    super(conn, relation, scan);
    initScanner();
  }

  public GCScanner(BaseIndexScanner base, ResultScanner scanner, byte[] indexFamily,
      byte[] indexQualifier) throws IOException {
    super(base);
    this.scanner = scanner;
    this.indexFamily = indexFamily;
    this.indexQualifier = indexQualifier;
  }

  /**
   * init selected range and scanner
   *
   * @throws IOException
   */
  private void initScanner() throws IOException {
    ScanRange selectedRange = null;
    int selectedRegionNumber = Integer.MAX_VALUE;
    for (ScanRange range : rangeList.getRanges()) {
      int cover = countCoveringRegions(conn,
          relation.getIndexTableName(range.getFamily(), range.getQualifier()), range.getStart(),
          range.getStop());
      LOG.info("LCDBG, " + cover + " regions are covered by range " + range);
      if (selectedRegionNumber > cover) {
        selectedRegionNumber = cover;
        selectedRange = range;
      }
    }
    LOG.info("LCDBG, GC Scanner using range " + selectedRange + " with " + selectedRegionNumber
        + " regions for scan id= " + rawScan.getId());
    indexFamily = selectedRange.getFamily();
    indexQualifier = selectedRange.getQualifier();
    List<ScanRange> list = new ArrayList<>(rangeList.getRanges());
    list.remove(selectedRange);
    Scan scan = new Scan();
    scan.setStartRow(selectedRange.getStart());
    scan.setStopRow(selectedRange.getStop());
    scan.setFamilyMap(rawScan.getFamilyMap());
    scan.setCaching(rawScan.getCaching());
    scan.setCacheBlocks(rawScan.getCacheBlocks());
    scan.setId(rawScan.getId());
    scan.setFilter(new ScanRange.ScanRangeList(list).toFilterList());
    Table table = conn.getTable(
        relation.getIndexTableName(selectedRange.getFamily(), selectedRange.getQualifier()));
    scanner = table.getScanner(scan);
  }

  @Override public Result next() throws IOException {
    if (scanner == null) return null;
    long timeStart = System.currentTimeMillis();
    Result res = scanner.next();
    totalScanTime += (System.currentTimeMillis() - timeStart);
    ++totalNumberOfRecords;
    return recoverClusteringResult(res, indexFamily, indexQualifier);
  }

  @Override public Result[] next(int nbRows) throws IOException {
    if (scanner == null) return null;
    long timeStart = System.currentTimeMillis();
    Result[] rawResults = scanner.next(nbRows);
    totalScanTime += (System.currentTimeMillis() - timeStart);
    totalNumberOfRecords += rawResults.length;
    Result[] ret = new Result[rawResults.length];
    for (int i = 0; i < ret.length; i++) {
      ret[i] = recoverClusteringResult(rawResults[i], indexFamily, indexQualifier);
    }
    return ret;
  }

  @Override public void printScanLatencyStatistic() {
    System.out.println(String.format(
        "GCScanner, means scan on clustering index, cost %.2f time to scan %d records for scan %s, average latency %.2f seconds",
        totalScanTime / 1000.0, totalNumberOfRecords, rawScan.getId(),
        totalNumberOfRecords / (totalScanTime / 1000.0)));
  }

  @Override public void close() {
    printScanLatencyStatistic();
    if (scanner != null) {
      scanner.close();
      scanner = null;
    }
  }
}
