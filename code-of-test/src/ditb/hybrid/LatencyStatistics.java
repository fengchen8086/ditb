package ditb.hybrid;

import paper.ResultParser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by winter on 17-3-27.
 */
public class LatencyStatistics {
  private long totalLatency = 0;
  private long maxLatency = 0;
  private long totalCount = 0;
  private int[] latencyBoxNumbers;

  public LatencyStatistics() {
    latencyBoxNumbers = new int[ResultParser.LatencyBoxPivots.length];
    for (int i = 0; i < latencyBoxNumbers.length; ++i) {
      latencyBoxNumbers[i] = 0;
    }
  }

  public void updateLatency(long timeInMS) {
    totalLatency += timeInMS;
    if (timeInMS > maxLatency) {
      maxLatency = timeInMS;
    }
    ++totalCount;
    double latency = timeInMS / 1000.0;
    if (ResultParser.LatencyBoxPivots[0] <= latency) {
      ++latencyBoxNumbers[0];
    }
    for (int i = ResultParser.LatencyBoxPivots.length - 2; i >= 0; i--) {
      if (ResultParser.LatencyBoxPivots[i] > latency) {
        ++latencyBoxNumbers[i + 1];
        return;
      }
    }
    ++latencyBoxNumbers[ResultParser.LatencyBoxPivots.length - 1];
  }

  public long getTotalLatency() {
    return totalLatency;
  }

  public double getMaxLatency() {
    return maxLatency / 1000.0;
  }

  public int[] getLatencyBoxNumbers() {
    return latencyBoxNumbers;
  }

  public List<String> calDetailLatency(String prefix) {
    List<String> latencyString = new ArrayList<>();
    int part = 0;
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < latencyBoxNumbers.length; ++i) {
      if (sb.length() > 500) {
        latencyString.add(sb.toString());
        sb = new StringBuilder();
        ++part;
      }
      if (sb.length() == 0) {
        sb.append(prefix).append(" part: ").append(part);
      }
      sb.append(", [").append(ResultParser.LatencyBoxPivots[i]).append("->")
          .append(String.format("%.3f%%", 100.0 * latencyBoxNumbers[i] / totalCount)).append("]");
    }
    sb.append(", [total count=").append(totalCount).append("]");
    latencyString.add(sb.toString());
    return latencyString;
  }

  public double getAvergeLatency() {
    return totalLatency / 1000.0 / totalCount;
  }

  public static WRSLatency mergeWRSLatencies(WRSLatency[] latencies) {
    WRSLatency ret = new WRSLatency();
    for (WRSLatency src : latencies) {
      // merge write latency
      if (src == null) {
        new RuntimeException("meet WRSLatency null").printStackTrace();
        continue;
      }
      for (int i = 0; i < src.writeStatistics.latencyBoxNumbers.length; i++) {
        ret.writeStatistics.latencyBoxNumbers[i] += src.writeStatistics.latencyBoxNumbers[i];
      }
      ret.writeStatistics.totalLatency += src.writeStatistics.totalLatency;
      ret.writeStatistics.totalCount += src.writeStatistics.totalCount;
      ret.writeStatistics.maxLatency =
          Math.max(ret.writeStatistics.maxLatency, src.writeStatistics.maxLatency);
      // merge read latency
      for (int i = 0; i < src.readStatistics.latencyBoxNumbers.length; i++) {
        ret.readStatistics.latencyBoxNumbers[i] += src.readStatistics.latencyBoxNumbers[i];
      }
      ret.readStatistics.totalLatency += src.readStatistics.totalLatency;
      ret.readStatistics.totalCount += src.readStatistics.totalCount;
      ret.readStatistics.maxLatency =
          Math.max(ret.readStatistics.maxLatency, src.readStatistics.maxLatency);
      // merge scan latency
      for (Map.Entry<String, Long> entry : src.scanTimes.entrySet()) {
        long prevTime =
            ret.scanTimes.containsKey(entry.getKey()) ? ret.scanTimes.get(entry.getKey()) : 0;
        long prevCount =
            ret.scanCounts.containsKey(entry.getKey()) ? ret.scanCounts.get(entry.getKey()) : 0;
        ret.scanTimes.put(entry.getKey(), prevTime + entry.getValue());
        ret.scanCounts.put(entry.getKey(), prevCount + src.scanCounts.get(entry.getKey()));
      }
    }
    return ret;
  }

  public static class WRSLatency {
    LatencyStatistics writeStatistics;
    LatencyStatistics readStatistics;
    Map<String, Long> scanTimes;
    Map<String, Long> scanCounts;

    public WRSLatency() {
      writeStatistics = new LatencyStatistics();
      readStatistics = new LatencyStatistics();
      scanTimes = new HashMap<>();
      scanCounts = new HashMap<>();
    }

    public void updateLatency(Operation operation, long timeInMS) {
      if (operation.isWrite()) {
        writeStatistics.updateLatency(timeInMS);
      } else if (operation.isRead()) {
        readStatistics.updateLatency(timeInMS);
      } else if (operation.isScan()) {
        String scanId = operation.getScan().getId();
        long prevTime = scanTimes.containsKey(scanId) ? scanTimes.get(scanId) : 0;
        long prevCount = scanCounts.containsKey(scanId) ? scanCounts.get(scanId) : 0;
        scanTimes.put(scanId, prevTime + timeInMS);
        scanCounts.put(scanId, prevCount + 1);
      }
    }
  }

  public static void main(String[] args) {
  }

}
