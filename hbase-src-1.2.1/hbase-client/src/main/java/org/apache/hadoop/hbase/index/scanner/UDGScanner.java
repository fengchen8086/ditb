package org.apache.hadoop.hbase.index.scanner;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.index.userdefine.ColumnInfo;
import org.apache.hadoop.hbase.index.userdefine.IndexRelationship;
import org.apache.hadoop.hbase.index.userdefine.IndexTableRelation;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Queue;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Scanner for User Defined Global Index
 */
public class UDGScanner extends BaseIndexScanner {

  BaseIndexScanner innerScanner;

  public UDGScanner(Connection conn, IndexTableRelation relation, Scan scan) throws IOException {
    super(conn, relation, scan);
    innerScanner = initScanner();
  }

  /**
   * t: target columns, f: filters, I: index relationship, C: Clustering Index(not full clustering), S: Secondary Index
   * 1. if C contains all t, and f is C.mainColumn, use C
   * 2. otherwise, use all indexes as secondary index
   *
   * @throws IOException
   */
  private BaseIndexScanner initScanner() throws IOException {
    // construct target family map
    TreeMap<byte[], TreeSet<byte[]>> targetFamilyMap;
    if (rawScan.getFamilyMap().isEmpty()) {
      targetFamilyMap = relation.getFamilyMap();
    } else {
      targetFamilyMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      for (Map.Entry<byte[], NavigableSet<byte[]>> entry : rawScan.getFamilyMap().entrySet()) {
        TreeSet<byte[]> set = new TreeSet<>(Bytes.BYTES_COMPARATOR);
        set.addAll(entry.getValue());
        targetFamilyMap.put(entry.getKey(), set);
      }
    }
    // check case 1, one indexrelationship contains all target columns and mainColumn is one of the filters
    ScanRange selectedRange = null;
    int targetCovering = Integer.MAX_VALUE;
    for (ScanRange sr : rangeList.getRanges()) {
      for (IndexRelationship one : relation.getIndexRelations()) {
        // check each index relation ship one by one
        if (one.getMainColumn().isSameColumn(sr.getFamily(), sr.getQualifier())) {
          TreeMap<byte[], TreeSet<byte[]>> unmatchedFamilyMap = copyMap(targetFamilyMap);
          removeColumnFromMap(unmatchedFamilyMap, sr.getFamily(), sr.getQualifier());
          for (ColumnInfo cl : one.getAdditionalColumns()) {
            removeColumnFromMap(unmatchedFamilyMap, cl.getFamily(), cl.getQualifier());
          }
          if (unmatchedFamilyMap.isEmpty()) {
            // all columns are match by this index!
            int cover = countCoveringRegions(conn,
                relation.getIndexTableName(sr.getFamily(), sr.getQualifier()), sr.getStart(),
                sr.getStop());
            if (cover < targetCovering) {
              targetCovering = cover;
              selectedRange = sr;
            }
          }
        }
      }
    }
    if (selectedRange != null) {
      // match case 1, targetRelation contains all t, and f is C.mainColumn, use it
      LOG.info("UDGScanner creating inner scanner of GC Scanner on " + selectedRange);
      return createGCScanner(selectedRange);
    }
    // otherwise, use all additional indexes as secondary index
    ScanRange.ScanRangeList list = new ScanRange.ScanRangeList();
    for (ScanRange sr : rangeList.getRanges()) {
      for (IndexRelationship one : relation.getIndexRelations()) {
        // check each index relation ship one by one
        if (one.getMainColumn().isSameColumn(sr.getFamily(), sr.getQualifier())) {
          list.addScanRange(sr);
          break;
        }
      }
    }
    LOG.info("UDGScanner creating inner scanner of GS Scanner on " + list);
    Queue<byte[]> rowkeyQueue = GSScanner
        .createRowkeyQueueBySecondaryIndex(conn, relation, rawScan.getFamilyMap(), list, rawScan);
    return new GSScanner(this, rowkeyQueue);
  }

  private GCScanner createGCScanner(ScanRange selectedRange) throws IOException {
    List<ScanRange> list = new ArrayList<>(rangeList.getRanges());
    list.remove(selectedRange);
    Scan scan = new Scan();
    scan.setStartRow(selectedRange.getStart());
    scan.setStopRow(selectedRange.getStop());
    scan.setCaching(rawScan.getCaching());
    scan.setCacheBlocks(rawScan.getCacheBlocks());
    scan.setFilter(new ScanRange.ScanRangeList(list).toFilterList());
    Table table = conn.getTable(
        relation.getIndexTableName(selectedRange.getFamily(), selectedRange.getQualifier()));
    ResultScanner scanner = table.getScanner(scan);
    return new GCScanner(this, scanner, selectedRange.getFamily(), selectedRange.getQualifier());
  }

  private TreeMap<byte[], TreeSet<byte[]>> copyMap(TreeMap<byte[], TreeSet<byte[]>> map) {
    TreeMap<byte[], TreeSet<byte[]>> copy =
        new TreeMap<byte[], TreeSet<byte[]>>(Bytes.BYTES_COMPARATOR);
    for (Map.Entry<byte[], TreeSet<byte[]>> entry : map.entrySet()) {
      TreeSet<byte[]> set = new TreeSet(Bytes.BYTES_COMPARATOR);
      set.addAll(entry.getValue());
      copy.put(entry.getKey(), set);
    }
    return copy;
  }

  private void removeColumnFromMap(TreeMap<byte[], TreeSet<byte[]>> map, byte[] family,
      byte[] qualifier) {
    TreeSet<byte[]> set = map.get(family);
    set.remove(qualifier);
    if (set.isEmpty()) map.remove(family);
  }

  @Override public Result next() throws IOException {
    if (innerScanner == null) return null;
    return innerScanner.next();
  }

  @Override public Result[] next(int nbRows) throws IOException {
    if (innerScanner == null) return null;
    return innerScanner.next(nbRows);
  }

  @Override public void printScanLatencyStatistic() {
    System.out.println("UDGScanner no latency statistic at all");
  }

  @Override public void close() {
    printScanLatencyStatistic();
    if (innerScanner != null) {
      innerScanner.close();
      innerScanner = null;
    }
  }
}
