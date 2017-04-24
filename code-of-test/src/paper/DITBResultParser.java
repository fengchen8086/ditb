package paper;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DITBResultParser {

  enum IndexType {
    NoIndex, GSIndex, CCIndex, IRIndex, LCIndex, MDIndex, LMDIndex_S, LMDIndex_D
  }

  public static final double[] LatencyBoxPivots =
      new double[] { 10, 6.0, 5.5, 5.0, 4.5, 4.0, 3.5, 3, 2.5, 2, 1.8, 1.5, 1.3, 1.2, 1.1, 1, 0.9,
          0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1, 0.08, 0.05, 0.02, 0.01, 0.0 };

  private static final class RunStatistics {
    // query cdf (Cumulative Distribution Function)
    public double[] pivots;
    public double[] writeLatencies;
    public double[] readLatencies;
    public int threadNumber = 0;
    public double avgWriteLatency = 0;
    public double avgReadLatency = 0;
    public double scanTotalTime = 0;
    public int scanTotalCount = 0;

    public RunStatistics() {
      pivots = new double[LatencyBoxPivots.length];
      writeLatencies = new double[LatencyBoxPivots.length];
      readLatencies = new double[LatencyBoxPivots.length];
    }

    public void calCDF() {
      for (int i = writeLatencies.length - 2; i >= 0; --i) {
        writeLatencies[i] = writeLatencies[i + 1] + writeLatencies[i];
        readLatencies[i] = readLatencies[i + 1] + readLatencies[i];
      }
    }
  }

  String inputFile = "/media/winter/E/result-data/ditb/R1-res-put-ditb.workload.A-20170403-222426";
//        String inputFile = "/media/winter/E/result-data/ditb/R1-res-put-ditb.workload.B-20170403-134714";
//        String inputFile = "/media/winter/E/result-data/ditb/R1-res-put-ditb.workload.C-20170331-004800";
  String outPutDir = "/tmp/ditb-results";
  String latencyCDFFile = outPutDir + "/insert-latency-CDF.dat";
  String insertTimeFile = outPutDir + "/insert-time.dat";
  String queryTimeFile = outPutDir + "/query-time.dat";

  IndexType[] indexTypes =
      new IndexType[] { IndexType.NoIndex, IndexType.GSIndex, IndexType.IRIndex, IndexType.CCIndex,
          IndexType.LCIndex, IndexType.MDIndex, };
  double maxPivot = 1.0;

  public static void main(String[] args) throws IOException {
    new DITBResultParser().doWork();
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
      IndexType indexType;
      if (line.startsWith("have")) {
        continue;
      } else if (line.startsWith("LMDIndex")) {
        indexType = IndexType.valueOf(line.substring(0, 10));
      } else {
        indexType = IndexType.valueOf(line.substring(0, 7));
      }
      //      System.out.println(indexType + " for line " + line);
      if (line.endsWith("write")) {
        updateWriteCDF(runStatMap, indexType, line.substring(0, line.length() - "write".length()));
      } else if (line.endsWith("read")) {
        updateReadCDF(runStatMap, indexType, line.substring(0, line.length() - "read".length()));
      } else if (line.endsWith("scan")) {
        updateScan(runStatMap, indexType, line.substring(0, line.length() - "scan".length()));
      }
    }

    System.out.println("**********process count");
    outputThreadCount(runStatMap);
    System.out.println("**********latency cdf");
    for (IndexType indexType : indexTypes)
      runStatMap.get(indexType).calCDF();
    outputWriteLatencyCDF(runStatMap);
    outputReadLatencyCDF(runStatMap);
    System.out.println("**********write latency");
    outputWriteLatency(runStatMap);
    System.out.println("**********read latency");
    outputReadLatency(runStatMap);
    System.out.println("**********scan time");
    outputScanTime(runStatMap);
    //    System.out.println("**********storage cost");
    //    outputStorageCost(runStatMap);
  }

  private void updateWriteCDF(Map<IndexType, RunStatistics> runStatMap, IndexType indexType,
      String line) {
    RunStatistics stat = getStat(runStatMap, indexType);
    String PARSING_STR_1 = "latency detail part:";
    if (line.indexOf(PARSING_STR_1) > -1) {
      line = line.substring(line.indexOf(PARSING_STR_1) + PARSING_STR_1.length() + 3);
      String parts[] = line.split(",");
      for (int i = 0; i < parts.length - 1; ++i) {
        // part = "[10.0->0.000%]";
        parts[i] = parts[i].trim();
        String[] ones = parts[i].substring(1, parts[i].length() - 2).split("->");
        stat.pivots[i] = Double.valueOf(ones[0]);
        stat.writeLatencies[i] += Double.valueOf(ones[1]);
      }
    } else {
      String pattern = "write latency, avg = (.*), max=";
      Pattern r = Pattern.compile(pattern);
      Matcher m = r.matcher(line);
      if (m.find()) {
        stat.avgWriteLatency += Double.valueOf(m.group(1));
      }
    }
  }

  private void updateReadCDF(Map<IndexType, RunStatistics> runStatMap, IndexType indexType,
      String line) {
    RunStatistics stat = getStat(runStatMap, indexType);
    String PARSING_STR_1 = "latency detail part:";
    if (line.indexOf(PARSING_STR_1) > -1) {
      line = line.substring(line.indexOf(PARSING_STR_1) + PARSING_STR_1.length() + 3);
      String parts[] = line.split(",");
      stat.threadNumber++;
      for (int i = 0; i < parts.length - 1; ++i) {
        // part = "[10.0->0.000%]";
        parts[i] = parts[i].trim();
        String[] ones = parts[i].substring(1, parts[i].length() - 2).split("->");
        stat.pivots[i] = Double.valueOf(ones[0]);
        stat.readLatencies[i] += Double.valueOf(ones[1]);
      }
    } else {
      String pattern = "read latency, avg = (.*), max=";
      Pattern r = Pattern.compile(pattern);
      Matcher m = r.matcher(line);
      if (m.find()) {
        stat.avgReadLatency += Double.valueOf(m.group(1));
      }
    }
  }

  private void outputWriteLatencyCDF(Map<IndexType, RunStatistics> runStatMap) throws IOException {
    BufferedWriter bw = new BufferedWriter(new FileWriter(latencyCDFFile));
    StringBuilder sb = new StringBuilder("write");
    for (IndexType indexType : indexTypes) {
      sb.append("\t").append(indexType);
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
        //        sb.append("\t").append(convert(stat.writeLatencies[i] / stat.threadNumber));
        sb.append("\t").append(convert(stat.writeLatencies[i] / stat.threadNumber));
      }
      System.out.println(sb.toString());
      bw.write(sb.toString() + "\n");
    }
    bw.close();
  }

  /**
   * out put latency cdf data
   *
   * @param runStatMap
   * @throws IOException
   */
  private void outputReadLatencyCDF(Map<IndexType, RunStatistics> runStatMap) throws IOException {
    BufferedWriter bw = new BufferedWriter(new FileWriter(latencyCDFFile));
    StringBuilder sb = new StringBuilder("read");
    for (IndexType indexType : indexTypes) {
      sb.append("\t").append(indexType);
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
        //        sb.append("\t").append(convert(stat.writeLatencies[i] / stat.threadNumber));
        sb.append("\t").append(convert(stat.readLatencies[i] / stat.threadNumber));
      }
      System.out.println(sb.toString());
      bw.write(sb.toString() + "\n");
    }
    bw.close();
  }

  private void updateScan(Map<IndexType, RunStatistics> runStatMap, IndexType indexType,
      String line) {
    String pattern = "scan time for (.*), avg =(.*) for (.*) times";
    Pattern r = Pattern.compile(pattern);
    Matcher m = r.matcher(line);
    if (m.find()) {
      getStat(runStatMap, indexType).scanTotalTime +=
          Double.valueOf(m.group(2)) * Integer.valueOf(m.group(3));
      getStat(runStatMap, indexType).scanTotalCount += Integer.valueOf(3);
    }
  }

  private void outputScanTime(Map<IndexType, RunStatistics> runStatMap) throws IOException {
    BufferedWriter bw = new BufferedWriter(new FileWriter(queryTimeFile));
    for (IndexType indexType : indexTypes) {
      StringBuilder sb = new StringBuilder(indexType.toString());
      RunStatistics stat = runStatMap.get(indexType);
      sb.append("\t").append(convert(stat.scanTotalTime / stat.scanTotalCount));
      System.out.println(sb.toString());
      bw.write(sb.toString() + "\n");
    }
    bw.close();
  }

  private void outputThreadCount(Map<IndexType, RunStatistics> runStatMap) throws IOException {
    for (IndexType indexType : indexTypes) {
      StringBuilder sb = new StringBuilder();
      RunStatistics stat = getStat(runStatMap, indexType);
      sb.append(indexType).append("\t").append(stat.threadNumber);
      System.out.println(sb.toString());
    }
  }

  private void outputWriteLatency(Map<IndexType, RunStatistics> runStatMap) throws IOException {
    BufferedWriter bw = new BufferedWriter(new FileWriter(insertTimeFile));
    RunStatistics base = runStatMap.get(IndexType.NoIndex);
    for (IndexType indexType : indexTypes) {
      StringBuilder sb = new StringBuilder();
      RunStatistics stat = getStat(runStatMap, indexType);
      sb.append(indexType).append("\t").append(convert(stat.avgWriteLatency / stat.threadNumber));
      //          .append(convert(runStatMap.get(indexType).insertTime / base.insertTime));
      System.out.println(sb.toString());
      bw.write(sb.toString() + "\n");
    }
    bw.close();
  }

  private void outputReadLatency(Map<IndexType, RunStatistics> runStatMap) throws IOException {
    BufferedWriter bw = new BufferedWriter(new FileWriter(insertTimeFile));
    RunStatistics base = runStatMap.get(IndexType.NoIndex);
    for (IndexType indexType : indexTypes) {
      StringBuilder sb = new StringBuilder();
      RunStatistics stat = getStat(runStatMap, indexType);
      sb.append(indexType).append("\t").append(convert(stat.avgReadLatency / stat.threadNumber));
      //          .append(convert(runStatMap.get(indexType).insertTime / base.insertTime));
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

  private String convert(Double d) {
    if (d == null) return "NaN";
    //    return String.format("%.02f", d);
    return String.format("%.04f", d);
  }
}