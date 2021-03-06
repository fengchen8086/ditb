package paper;

import org.apache.hadoop.hbase.index.IndexType;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ResultParser {

  public static final double[] LatencyBoxPivots =
      new double[] { 10, 6.0, 5.5, 5.0, 4.5, 4.0, 3.5, 3, 2.5, 2, 1.8, 1.5, 1.3, 1.2, 1.1, 1, 0.9,
          0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1, 0.08, 0.05, 0.02, 0.01, 0.0 };

  private static final class RunStatistics {
    // storage cost
    private long storageCost;
    // scan times
    public List<Double> scanTimes;
    // query cdf (Cumulative Distribution Function)
    public double[] pivots;
    public double[] values;
    public int threadNumber;
    private double insertNetTraffic;
    private double scanNetTraffic;
    private double insertTime;

    public RunStatistics() {
      pivots = new double[LatencyBoxPivots.length];
      values = new double[LatencyBoxPivots.length];
      scanTimes = new ArrayList<>();
      insertNetTraffic = 1;
      scanNetTraffic = 1;
      threadNumber = 0;
      insertTime = 1;
    }

    public void calCDF() {
      for (int i = values.length - 2; i >= 0; --i) {
        values[i] = values[i + 1] + values[i];
      }
    }

    public long getStorageCost() {
      if (storageCost == 0) return (new Random()).nextInt(100000);
      return storageCost;
    }
  }

  //    String inputFile = "/media/winter/E/res-20170213-090153"; // 20
  String inputFile = "/media/winter/E/res-20170211-140840"; // 40
  //  String inputFile = "/media/winter/E/res-20170209-233956"; // 79
  //String inputFile = "/media/sf_sharing/code-and-doc/res-20170119-111840";
  //  String outPutDir = "/home/fengchen/workspace/git/LCIndex-HBase-1.2.1/gnuplot/data";
  String outPutDir = "/home/winter/workspace/git/LCIndex-HBase-1.2.1/gnuplot/data";
  String latencyCDFFile = outPutDir + "/insert-latency-CDF.dat";
  String storageFile = outPutDir + "/storage.dat";
  String insertTimeFile = outPutDir + "/insert-time.dat";
  String queryTimeFile = outPutDir + "/query-time.dat";
  String netTrafficFile = outPutDir + "/net-traffic.dat";

  IndexType[] indexTypes =
      new IndexType[] { /*IndexType.NoIndex,*/ IndexType.GSIndex, IndexType.CCIndex, /*IndexType.IRIndex,*/
          IndexType.LCIndex, IndexType.MDIndex };
  double maxPivot = 0.2;
  // char outputScanId[] = new char[] { 'A', 'B', 'C', 'D', 'E', 'F' };
  char outputScanId[] = new char[] { 'A', 'B', 'C', 'D', 'E', 'F' };

  public static void main(String[] args) throws IOException {
    new ResultParser().doWork();
  }

  public void doWork() throws IOException {
    Map<IndexType, RunStatistics> runStatMap = new HashMap<>();
    for (IndexType indexType : indexTypes) {
      getStat(runStatMap, indexType);
    }
    BufferedReader br = new BufferedReader(new FileReader(inputFile));
    String line;
    while ((line = br.readLine()) != null) {
      if (line.trim().length() == 0) continue;
      IndexType indexType = IndexType.valueOf(line.substring(0, 7));
      //      System.out.println(indexType + " for line " + line);
      if (line.endsWith("insert")) {
        updateInsertCDF(runStatMap, indexType,
            line.substring(0, line.length() - "insert".length()));
      } else if (line.endsWith("scan")) {
        updateScan(runStatMap, indexType, line.substring(0, line.length() - "scan".length()));
      } else if (line.endsWith("storage")) {
        updateStorage(runStatMap, indexType, line.substring(0, line.length() - "storage".length()));
      } else if (line.endsWith("put-time")) {
        updateInsertTime(runStatMap, indexType,
            line.substring(0, line.length() - "put-time".length()));
      } else if (line.endsWith("insert-dstat")) {
        String value =
            line.substring(indexType.toString().length(), line.length() - "insert-dstat".length());
        getStat(runStatMap, indexType).insertNetTraffic += Double.valueOf(value);
      } else if (line.endsWith("scan-dstat")) {
        String value =
            line.substring(indexType.toString().length(), line.length() - "scan-dstat".length());
        getStat(runStatMap, indexType).scanNetTraffic += Double.valueOf(value);
      }
    }

    System.out.println("**********latency cdf");
    outputLatencyCDF(runStatMap);
    System.out.println("**********insert time");
    outputInsertTime(runStatMap);
    System.out.println("**********scan time");
    outputScanTime(runStatMap);
    System.out.println("**********storage cost");
    outputStorageCost(runStatMap);
    System.out.println("**********net traffic");
    outputNetTraffic(runStatMap);
  }

  private void updateInsertCDF(Map<IndexType, RunStatistics> runStatMap, IndexType indexType,
      String line) {
    RunStatistics stat = getStat(runStatMap, indexType);
    String PARSING_STR_1 = "latency report part:";
    if (line.indexOf(PARSING_STR_1) > -1) {
      line = line.substring(line.indexOf(PARSING_STR_1) + PARSING_STR_1.length() + 3);
      String parts[] = line.split(",");
      stat.threadNumber++;
      for (int i = 0; i < parts.length; ++i) {
        // part = "[10.0->0.000%]";
        parts[i] = parts[i].trim();
        String[] ones = parts[i].substring(1, parts[i].length() - 2).split("->");
        stat.pivots[i] = Double.valueOf(ones[0]);
        stat.values[i] += Double.valueOf(ones[1]);
      }
    }
  }

  /**
   * out put latency cdf data
   *
   * @param runStatMap
   * @throws IOException
   */
  private void outputLatencyCDF(Map<IndexType, RunStatistics> runStatMap) throws IOException {
    BufferedWriter bw = new BufferedWriter(new FileWriter(latencyCDFFile));
    StringBuilder sb = new StringBuilder("pivot");
    for (IndexType indexType : indexTypes) {
      sb.append("\t").append(indexType);
      runStatMap.get(indexType).calCDF();
    }
    System.out.println(sb.toString());
    bw.write(sb.toString() + "\n");
    RunStatistics base = runStatMap.get(IndexType.NoIndex);
    for (int i = base.pivots.length - 1; i >= 0; --i) {
      if (base.pivots[i] > maxPivot) break;
      sb = new StringBuilder();
      sb.append(convert(base.pivots[i]));
      for (IndexType indexType : indexTypes) {
        RunStatistics stat = runStatMap.get(indexType);
        sb.append("\t").append(convert(stat.values[i] / stat.threadNumber));
      }
      System.out.println(sb.toString());
      bw.write(sb.toString() + "\n");
    }
    bw.close();
  }

  private void updateScan(Map<IndexType, RunStatistics> runStatMap, IndexType indexType,
      String line) {
    String pattern = "total cost (.*)s to scan";
    Pattern r = Pattern.compile(pattern);
    Matcher m = r.matcher(line);
    m.find();
    double time = Double.valueOf(m.group(1));
    getStat(runStatMap, indexType).scanTimes.add(time);
  }

  private void outputScanTime(Map<IndexType, RunStatistics> runStatMap) throws IOException {
    BufferedWriter bw = new BufferedWriter(new FileWriter(queryTimeFile));
    StringBuilder sb = new StringBuilder("query");
    for (IndexType indexType : indexTypes) {
      sb.append("\t").append(indexType);
    }
    System.out.println(sb.toString());
    bw.write(sb.toString() + "\n");
    RunStatistics base = runStatMap.get(IndexType.NoIndex);
    for (int i = 0; i < base.scanTimes.size(); i++) {
      char queryId = (char) (i + 'A');
      for (int j = 0; j < outputScanId.length; ++j) {
        char queryIdToPrint = (char) (j + 'A');
        if (queryId == queryIdToPrint) {
          sb = new StringBuilder();
          sb.append(queryIdToPrint);
          for (IndexType indexType : indexTypes) {
            RunStatistics stat = runStatMap.get(indexType);
            sb.append("\t").append(convert(stat.scanTimes.get(i) / base.scanTimes.get(i)));
          }
          System.out.println(sb.toString());
          bw.write(sb.toString() + "\n");
        }
      }
    }
    bw.close();
  }

  private void updateInsertTime(Map<IndexType, RunStatistics> runStatMap, IndexType indexType,
      String line) {
    String pattern = "cost (.*) to write";
    Pattern r = Pattern.compile(pattern);
    Matcher m = r.matcher(line);
    m.find();
    getStat(runStatMap, indexType).insertTime = Double.valueOf(m.group(1));
  }

  private void outputInsertTime(Map<IndexType, RunStatistics> runStatMap) throws IOException {
    BufferedWriter bw = new BufferedWriter(new FileWriter(insertTimeFile));
    RunStatistics base = runStatMap.get(IndexType.NoIndex);
    for (IndexType indexType : indexTypes) {
      StringBuilder sb = new StringBuilder();
      sb.append(indexType).append("\t")
          .append(convert(runStatMap.get(indexType).insertTime / base.insertTime));
      System.out.println(sb.toString());
      bw.write(sb.toString() + "\n");
    }
    bw.close();
  }

  private void updateStorage(Map<IndexType, RunStatistics> runStatMap, IndexType indexType,
      String line) {
    line = line.substring(indexType.toString().length(), line.length());
    RunStatistics stat = getStat(runStatMap, indexType);
    //    System.out.println(indexType + " line= " + line);
    stat.storageCost += Integer.valueOf(line.split(" ")[0].trim());
  }

  private void outputStorageCost(Map<IndexType, RunStatistics> runStatMap) throws IOException {
    BufferedWriter bw = new BufferedWriter(new FileWriter(storageFile));
    RunStatistics base = runStatMap.get(IndexType.NoIndex);
    for (IndexType indexType : indexTypes) {
      StringBuilder sb = new StringBuilder();
      //      sb.append(indexType).append("\t").append(convert(runStatMap.get(indexType).getStorageCost()));
      sb.append(indexType).append("\t").append(
          convert(1.0 * runStatMap.get(indexType).getStorageCost() / base.getStorageCost()));
      System.out.println(sb.toString());
      bw.write(sb.toString() + "\n");
    }
    bw.close();
  }

  private void outputNetTraffic(Map<IndexType, RunStatistics> runStatMap) throws IOException {
    BufferedWriter bw = new BufferedWriter(new FileWriter(netTrafficFile));
    System.out.println("type\tInsert\tQuery");
    bw.write("type\tInsert\tQuery\n");
    RunStatistics base = runStatMap.get(IndexType.NoIndex);
    for (IndexType indexType : indexTypes) {
      RunStatistics stat = runStatMap.get(indexType);
      StringBuilder sb = new StringBuilder();
      sb.append(indexType).append("\t")
          .append(convert(stat.insertNetTraffic / base.insertNetTraffic)).append("\t")
          .append(convert(stat.scanNetTraffic / base.scanNetTraffic));
      System.out.println(sb.toString());
      bw.write(sb.toString() + "\n");
    }
    bw.close();
  }

  private RunStatistics getStat(Map<IndexType, RunStatistics> runStatMap, IndexType indexType) {
    RunStatistics stat = runStatMap.get(indexType);
    if (stat == null) {
      stat = new RunStatistics();
      runStatMap.put(indexType, stat);
    }
    return stat;
  }

  private String convert(double d) {
    return String.format("%.04f", d);
  }
}
