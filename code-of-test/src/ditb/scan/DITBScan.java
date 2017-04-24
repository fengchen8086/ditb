package ditb.scan;

import ditb.workload.AbstractWorkload;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.index.IndexType;
import org.apache.hadoop.hbase.index.scanner.ScanRange;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by winter on 17-1-11.
 */
public class DITBScan {

  final boolean PRINT_RESULT = false;
  final int reportInterval = 5000;
  AbstractWorkload workload;
  final IndexType indexType;

  class ScanResult {
    long totalTime = 0;
    long nbResults = 0;
    long firstLatency = -1;
  }

  public DITBScan(IndexType indexType, String workloadClassName, String workloadDesc)
      throws IOException, InterruptedException {
    this.indexType = indexType;
    workload = AbstractWorkload.getWorkload(workloadClassName, workloadDesc);
  }

  private void work() throws IOException, InterruptedException {
    for (File file : workload.getScanFilterFiles()) {
      ArrayList<ScanResult> list = new ArrayList<>();
      long totalTime = 0, totalFirstLatency = 0;
      for (int i = 0; i < workload.getScanRunTimes(); i++) {
        ScanResult scanResult =
            runOneTime(indexType, file.getAbsolutePath(), workload.getHBaseConfiguration(),
                workload.getScanCacheSize(), workload);
        list.add(scanResult);
        System.out.println("report scan time " + i + " on index " + indexType + ", total cost "
            + scanResult.totalTime / 1000.0 + "s to scan " + scanResult.nbResults
            + " results, first latency is: " + scanResult.firstLatency + ", desc=" + file
            .getName());
        totalTime += scanResult.totalTime;
        totalFirstLatency += scanResult.firstLatency;
      }
      System.out.println(
          "report scan avg, total time= " + totalTime / 1000.0 + "s, avg first latency is: "
              + totalFirstLatency / 1000.0 / list.size() + ", scan file=" + file.getName());
    }
  }

  private ScanResult runOneTime(IndexType indexType, String fileName, Configuration conf,
      int cacheSize, AbstractWorkload workload) throws IOException {
    ScanRange.ScanRangeList rangeList = ScanRange.ScanRangeList.getScanRangeList(fileName);
    System.out.println("scan range in file " + fileName + " is: " + rangeList);
    DITBScanBase scanner;
    if (indexType == IndexType.MDIndex) {
      scanner = new DITBMDIndexScanner(workload.getTableName(), indexType, workload);
    } else {
      scanner = new DITBNormalScanner(workload.getTableName(), indexType, workload);
    }
    Scan scan = ScanRange.ScanRangeList.getScan(fileName);
    scan.setCacheBlocks(false);
    scan.setCaching(cacheSize);
    scan.setId(fileName);
    System.out.println("scan filter " + scan.getFilter());
    //    ScanResult scanResult = new ScanResult(fileName + ": " + rangeList);
    ScanResult scanResult = new ScanResult();
    int reportCount = 0;
    Result[] results;
    long timeStart = System.currentTimeMillis();
    ResultScanner resultScanner = scanner.getScanner(scan);
    try {
      while (true) {
        results = resultScanner.next(cacheSize);
        if (scanResult.firstLatency == -1)
          scanResult.firstLatency = System.currentTimeMillis() - timeStart;
        if (results == null || results.length == 0) break;
        if (PRINT_RESULT) {
          for (Result result : results)
            System.out.println(workload.parseResult(result));
        }
        scanResult.nbResults += results.length;
        reportCount += results.length;
        if (reportCount >= reportInterval) {
          System.out.println(
              "finish scan for " + results.length + " records, " + scanResult.nbResults
                  + " in total");
          reportCount = 0;
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    scanResult.totalTime = System.currentTimeMillis() - timeStart;
    if (scanResult.nbResults == 0) scanResult.nbResults = 1;
    return scanResult;
  }

  public static void usage() {
    System.out.println("DITBScan indexType workload_cls_name workload_desc_path");
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    if (args.length < 3) {
      usage();
      return;
    }
    new DITBScan(IndexType.valueOf(args[0]), args[1], args[2]).work();
  }
}
