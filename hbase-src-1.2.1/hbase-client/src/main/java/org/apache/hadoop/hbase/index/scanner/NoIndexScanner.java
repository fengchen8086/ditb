package org.apache.hadoop.hbase.index.scanner;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.index.userdefine.IndexTableRelation;

import java.io.IOException;

/**
 * Created by fengchen on 17-1-12.
 */
public class NoIndexScanner extends BaseIndexScanner {

  ResultScanner scanner;

  public NoIndexScanner(Connection conn, IndexTableRelation relation, Scan scan)
      throws IOException {
    super(conn, relation, scan);
    scanner = conn.getTable(relation.getTableName()).getScanner(scan);
  }

  @Override public Result next() throws IOException {
    return scanner == null ? null : scanner.next();
  }

  @Override public Result[] next(int nbRows) throws IOException {
    return scanner == null ? null : scanner.next(nbRows);
  }

  @Override public void printScanLatencyStatistic() {
    System.out.println("NoIndexScanner, not stat presented");
  }

  @Override public void close() {
    printScanLatencyStatistic();
    if (scanner != null) scanner.close();
  }
}
