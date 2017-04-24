package userdefined;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.index.IndexType;
import org.apache.hadoop.hbase.index.client.DataType;
import org.apache.hadoop.hbase.index.scanner.BaseIndexScanner;
import org.apache.hadoop.hbase.index.scanner.ScanRange;
import org.apache.hadoop.hbase.index.userdefine.IndexTableAdmin;
import org.apache.hadoop.hbase.index.userdefine.IndexTableRelation;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.index.lmdindex.LMDIndexConstants;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by winter on 16-12-29.
 */
public class LMDTester {
  private static final TableName tableName = TableName.valueOf("tbl_lmd");
  private static final byte[] familyName = Bytes.toBytes("f");
  String confFile =
      "/home/winter/workspace/git/LCIndex-HBase-1.2.1/scripts/uni/conf/winter-hbase.conf";
  String[] indexColumnNames = new String[] { "a", "b", "c" };
  int dataColumnNumber = 50;

  private Configuration conf;
  private Connection conn;
  private Admin hBaseAdmin;
  IndexTableRelation relation;
  IndexTableAdmin admin;
  boolean PRINT_RESULT = true;

  int RECORD_NUMBER = 100;
  int BUCKET_SIZE = 10;
  //  int[][] scanValues = new int[][] { { 5, 10 }, { 5, 10 }, { 5, 10 } };
  int[][] scanValues = new int[][] { { 5, 10 } };
  //  int[][] scanValues =
  //      new int[][] { { 5, RECORD_NUMBER / 2 }, { 5, RECORD_NUMBER / 2 }, { 5, RECORD_NUMBER / 2 } };
  boolean FLUSH_TABLE = true;
  int INSERT_LIST_SIZE = 10;
  //    IndexType indexType = IndexType.LMDIndex_D;
  IndexType indexType = IndexType.LMDIndex_S;

  private void work() throws IOException {
    init();
    clearTable();
    doInsert(RECORD_NUMBER, Integer.MAX_VALUE, FLUSH_TABLE);
    doRawScan();
    doIndexScan();
    admin.close();
  }

  private void doIndexScan() throws IOException {
    ScanRange.ScanRangeList rangeList = new ScanRange.ScanRangeList();
    FilterList filterList = new FilterList();
    CompareFilter.CompareOp startOp = CompareFilter.CompareOp.GREATER_OR_EQUAL;
    CompareFilter.CompareOp stopOp = CompareFilter.CompareOp.LESS_OR_EQUAL;
    for (int i = 0; i < indexColumnNames.length && i < scanValues.length; i++) {
      rangeList.addScanRange(new ScanRange(familyName, Bytes.toBytes(indexColumnNames[i]),
          Bytes.toBytes(scanValues[i][0]), Bytes.toBytes(scanValues[i][1]), startOp, stopOp,
          DataType.INT));
      filterList.addFilter(
          new SingleColumnValueFilter(familyName, Bytes.toBytes(indexColumnNames[i]), startOp,
              Bytes.toBytes(scanValues[i][0])));
      filterList.addFilter(
          new SingleColumnValueFilter(familyName, Bytes.toBytes(indexColumnNames[i]), stopOp,
              Bytes.toBytes(scanValues[i][1])));
    }
    Scan scan = new Scan();
    scan.setFilter(filterList);
    if (rangeList.getRanges().size() > 0) {
      scan.setAttribute(ScanRange.SCAN_RANGE_ATTRIBUTE_STR, rangeList.toBytesAttribute());
    }
    scan.setId("LMD-scan");
    scan.setCaching(1);
    ResultScanner scanner = BaseIndexScanner.getIndexScanner(conn, relation, scan);
    Result result;
    int count = 0;
    while ((result = scanner.next()) != null) {
      count++;
      if (PRINT_RESULT) printResult(result);
    }
    scanner.close();
    System.out.println("LMDIndex scan has " + count + " records");
  }

  private void doRawScan() throws IOException {
    FilterList filterList = new FilterList();
    CompareFilter.CompareOp startOp = CompareFilter.CompareOp.GREATER_OR_EQUAL;
    CompareFilter.CompareOp stopOp = CompareFilter.CompareOp.LESS_OR_EQUAL;
    for (int i = 0; i < indexColumnNames.length && i < scanValues.length; i++) {
      filterList.addFilter(
          new SingleColumnValueFilter(familyName, Bytes.toBytes(indexColumnNames[i]), startOp,
              Bytes.toBytes(scanValues[i][0])));
      filterList.addFilter(
          new SingleColumnValueFilter(familyName, Bytes.toBytes(indexColumnNames[i]), stopOp,
              Bytes.toBytes(scanValues[i][1])));
    }
    Scan scan = new Scan();
    scan.setFilter(filterList);
    scan.setId("raw-scan");
    Table table = conn.getTable(tableName);
    ResultScanner scanner = table.getScanner(scan);
    Result result;
    int count = 0;
    while ((result = scanner.next()) != null) {
      ++count;
      if (PRINT_RESULT) printResult(result);
    }
    scanner.close();
    System.out.println("raw scan has " + count + " records");
  }

  private void doInsert(int totalSize, int flushSize, boolean flushTable) throws IOException {
    Table table = conn.getTable(tableName);
    List<Put> putList = new ArrayList<>();
    for (int i = 0; i < totalSize; i++) {
      Put put = new Put(Bytes.toBytes(i));
      for (int j = 0; j < indexColumnNames.length; j++) {
        put.addColumn(familyName, Bytes.toBytes(indexColumnNames[j]),
            Bytes.toBytes(i % BUCKET_SIZE));
      }
      for (int j = 0; j < dataColumnNumber; j++) {
        put.addColumn(familyName, Bytes.toBytes("info"), Bytes.toBytes(i * 10));
      }
      putList.add(put);
      if (putList.size() >= INSERT_LIST_SIZE) {
        table.put(putList);
        putList = new ArrayList<>(INSERT_LIST_SIZE);
      }
      if (i > flushSize) {
        hBaseAdmin.flush(tableName);
        flushSize = Integer.MAX_VALUE;
        System.out.println("flush table after " + i);
        try {
          Thread.sleep(3 * 1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
    table.close();
    if (flushTable) hBaseAdmin.flush(tableName);

    System.out.println("insert " + totalSize + " records into table, flush table=" + flushTable);
  }

  private void init() throws IOException {
    conf = HBaseConfiguration.create();
    HRegionServer.loadWinterConf(conf, confFile);
    conn = ConnectionFactory.createConnection(conf);
    hBaseAdmin = conn.getAdmin();
    relation = new IndexTableRelation(tableName, indexType);
    for (String col : indexColumnNames) {
      relation.addIndexColumn(familyName, Bytes.toBytes(col));
      relation.addColumn(familyName, Bytes.toBytes(col));
    }
    for (int i = 0; i < dataColumnNumber; i++) {
      relation.addColumn(familyName, Bytes.toBytes(i));
    }
    admin = new IndexTableAdmin(conf, conn, relation);
  }

  public void clearTable() throws IOException {
    admin.setLMDIndexThreshold(BUCKET_SIZE);
    admin.createTable(true, true);
    System.out.println("create table " + tableName + " success!");
  }

  private void printResult(Result result) {
    long rowkey = Bytes.toInt(result.getRow());
    //    int a = Bytes.toInt(result.getValue(familyName, Bytes.toBytes("a")));
    //    int b = Bytes.toInt(result.getValue(familyName, Bytes.toBytes("b")));
    //    int c = Bytes.toInt(result.getValue(familyName, Bytes.toBytes("c")));
    //    int info = Bytes.toInt(result.getValue(familyName, Bytes.toBytes("info")));
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    sb.append("rowkey=").append(rowkey).append(",");

    for (Cell cell : result.listCells()) {
      sb.append(Bytes.toString(cell.getQualifier())).append("=")
          .append(Bytes.toInt(cell.getValue())).append(",");
    }

    //    sb.append("a=").append(a).append(",");
    //    sb.append("b=").append(b).append(",");
    //    sb.append("c=").append(c).append(",");
    //    sb.append("info=").append(info).append(",");
    sb.append("}");
    System.out.println(sb.toString());
  }

  public static void main(String[] args) throws IOException {
    new LMDTester().work();
  }
}
