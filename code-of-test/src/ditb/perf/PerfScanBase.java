package ditb.perf;

import ditb.util.DITBUtil;
import ditb.workload.AbstractWorkload;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.index.IndexType;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by winter on 17-3-11.
 */
public abstract class PerfScanBase {

  // operators
  protected Configuration conf;
  protected Connection conn;
  protected Admin admin;
  protected AbstractWorkload workload;
  protected IndexType indexType;
  protected TableName opTblName;

  private String queryFilePath;
  private String scanFilePath;

  private int nbGet;
  private int nbScan;
  private int sizeScanCovering;
  private int testTimes;

  private final int SEQ_ORDER_LIST_SIZE = 10000;

  public PerfScanBase(IndexType indexType, AbstractWorkload workload, String dataDstDir, int nbGet,
      int nbScan, int sizeScanCovering, int testTimes) throws IOException {
    this.indexType = indexType;
    this.workload = workload;
    queryFilePath = DITBUtil.getChildPath(dataDstDir, PerfDataGenerator.QUERY_FILE_NAME);
    scanFilePath = DITBUtil.getChildPath(dataDstDir, PerfDataGenerator.SCAN_FILE_NAME);
    this.nbGet = nbGet;
    this.nbScan = nbScan;
    this.sizeScanCovering = sizeScanCovering;
    this.testTimes = testTimes;
    conf = workload.getHBaseConfiguration();
    conn = ConnectionFactory.createConnection(conf);
    // init table
    admin = conn.getAdmin();
  }

  private void work() throws IOException, ParseException {
    opTblName = getOpTableName();
    for (int i = 0; i < testTimes; i++) {
      List<OpResult> results = new ArrayList<>();
      results.add(executeRandomGet());
      results.add(executeSequenceGet());
      results.add(executeScan());
      StringBuilder sb = new StringBuilder();
      sb.append("report scan").append(indexType).append(" query latency test time ").append("#")
          .append(i);
      for (OpResult result : results) {
        sb.append(", ").append(result.toString());
      }
      System.out.println(sb.toString());
      sb = new StringBuilder();
      sb.append("report scan").append(indexType);
      for (OpResult result : results) {
        sb.append(", ").append(result.getAvgLatency());
      }
      System.out.println(sb.toString());
    }
  }

  private OpResult executeRandomGet() throws IOException, ParseException {
    if (!hasRandomGet()) {
      return new OpResult("random get not supported", 1, 1);
    }
    Table table = conn.getTable(opTblName);
    BufferedReader br = new BufferedReader(new FileReader(queryFilePath));
    String line;
    int counter = 0;
    long totalTime = 0;
    while ((line = br.readLine()) != null) {
      Get get = getIndexTableGet(line);
      long startTime = System.currentTimeMillis();
      Result result = processGet(table, get);
      totalTime += System.currentTimeMillis() - startTime;
      counter += recordsInOneResult(result);
      if (counter >= nbGet) break;
    }
    OpResult ret = new OpResult("random get", counter, totalTime);
    br.close();
    table.close();
    return ret;
  }

  private OpResult executeSequenceGet() throws IOException, ParseException {
    if (!hasSequenceGet()) {
      return new OpResult("sequence get not supported", 1, 1);
    }
    Table table = conn.getTable(opTblName);
    BufferedReader br = new BufferedReader(new FileReader(queryFilePath));
    String line;
    int counter = 0;
    long totalTime = 0;
    List<Get> list = new ArrayList<>(SEQ_ORDER_LIST_SIZE);
    while ((line = br.readLine()) != null) {
      if (list.size() < SEQ_ORDER_LIST_SIZE) {
        Get get = getIndexTableGet(line);
        list.add(get);
      } else {
        // sort and then execute gets
        Collections.sort(list);
        for (Get get : list) {
          long startTime = System.currentTimeMillis();
          Result result = processGet(table, get);
          totalTime += System.currentTimeMillis() - startTime;
          counter += recordsInOneResult(result);
          if (counter >= nbGet) break;
        }
        list = new ArrayList<>();
      }
    }
    Collections.sort(list);
    for (Get get : list) {
      long startTime = System.currentTimeMillis();
      Result result = processGet(table, get);
      totalTime += System.currentTimeMillis() - startTime;
      counter += recordsInOneResult(result);
      if (counter >= nbGet) break;
    }
    OpResult ret = new OpResult("sequence get", counter, totalTime);
    br.close();
    table.close();
    return ret;
  }

  protected Result processGet(Table table, Get get) throws IOException {
    get.setCacheBlocks(false);
    return table.get(get);
  }

  private OpResult executeScan() throws IOException, ParseException {
    if (!hasScan()) {
      return new OpResult("scan not supported", 1, 1);
    }
    Table table = conn.getTable(opTblName);
    BufferedReader br = new BufferedReader(new FileReader(scanFilePath));
    String line;
    long totalTime = 0;
    int counter = 0;
    Result[] results;
    while ((line = br.readLine()) != null) {
      Scan scan = new Scan(getIndexTableScanStartKey(line));
      scan.setCaching(workload.getScanCacheSize());
      scan.setCacheBlocks(false);
      long startTime = System.currentTimeMillis();
      ResultScanner scanner = table.getScanner(scan);
      int wantedRecords = sizeScanCovering;
      while (true) {
        results = scanner.next(Math.min(wantedRecords, workload.getScanCacheSize()));
        if (results == null || results.length == 0) break;
        for (Result result : results) {
          int k = recordsInOneResult(result);
          wantedRecords -= k;
          counter += k;
        }
        if (wantedRecords <= 0) break;
      }
      scanner.close();
      totalTime += System.currentTimeMillis() - startTime;
    }
    OpResult ret = new OpResult("scan", counter, totalTime);
    br.close();
    table.close();
    return ret;
  }

  protected abstract TableName getOpTableName();

  protected abstract int recordsInOneResult(Result result);

//  protected abstract Put getIndexTablePut(String line) throws ParseException, IOException;

  protected abstract Get getIndexTableGet(String line) throws IOException, ParseException;

  protected abstract boolean hasRandomGet();

  protected abstract boolean hasSequenceGet();

  protected abstract boolean hasScan();

  protected abstract byte[] getIndexTableScanStartKey(String line)
      throws IOException, ParseException;

  class OpResult {
    String name;
    long costTime;
    int records;

    public OpResult(String name, int nbRecords, long costTime) {
      this.name = name;
      this.costTime = costTime;
      this.records = nbRecords;
    }

    @Override public String toString() {
      return String.format("{%s: %d records in %.3f seconds, avg latency=%.7f}", name, records,
          costTime / 1000.0, costTime / 1000.0 / records);
    }

    public String getAvgLatency() {
      return String.format("%.7f", costTime / 1000.0 / records);
    }

  }

  public static PerfScanBase getProperPerfScan(IndexType indexType, String workloadClsName,
      String workloadDesc, String dataDstDir, int nbGet, int nbScan, int sizeScanCovering,
      int testTimes) throws IOException {
    AbstractWorkload workload = AbstractWorkload.getWorkload(workloadClsName, workloadDesc);
    if (IndexType.isMDPerf(indexType)) {
      return new PerfMD(workload.getTableName(), indexType, workload)
          .getPerfScan(dataDstDir, nbGet, nbScan, sizeScanCovering, testTimes);
    } else {
      return new PerfNormal(workload.getTableName(), indexType, workload)
          .getPerfScan(dataDstDir, nbGet, nbScan, sizeScanCovering, testTimes);
    }
  }

  public static void usage() {
    System.out.println(
        "indexType, workloadClsName, workloadDesc, dataDstDir, nbGet, nbScan, sizeScanCovering, testTimes");
  }

  public static void main(String[] args) throws IOException, ParseException {
    if (args.length < 8) {
      StringBuilder sb = new StringBuilder();
      sb.append("current parameters: ");
      for (String one : args) {
        sb.append(one).append(",");
      }
      usage();
      return;
    }
    PerfScanBase scan = getProperPerfScan(IndexType.valueOf(args[0]), args[1], args[2], args[3],
        Integer.valueOf(args[4]), Integer.valueOf(args[5]), Integer.valueOf(args[6]),
        Integer.valueOf(args[7]));
    scan.work();
  }

}
