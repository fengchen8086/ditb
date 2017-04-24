package ditb.perf;

import ditb.util.DITBUtil;
import ditb.workload.AbstractDITBRecord;
import ditb.workload.AbstractWorkload;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import paper.ResultParser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public abstract class PerfInserterBase {
  public abstract void close() throws IOException;

  // data generate and table control
  protected final String PERCENT_FORMAT = "%.3f%%";

  protected final int[] globalBoxNumber;
  protected final Object syncBoxObj = new Object();

  // hbase operation
  protected final TableName tableName;
  protected final Configuration conf;

  // multiple thread
  final int threadNum;
  final String loadDataDir;
  final String statFilePath;
  boolean[] threadFinishMark;
  double[] threadLatency;
  AtomicLong maxLatency = new AtomicLong(0);
  final int reportInterval = 10000;
  final Queue<String> reportQueue;
  FinishCounter finishCounter = new FinishCounter(0);
  RunnableDataLoader[] loaders;
  RunnablePerfInserter[] inserters;
  int processId;
  int totalDoneSize = 0;
  AbstractWorkload workload;

  public PerfInserterBase(Configuration conf, TableName tableName, String loadDataDir,
      int processId, int threadNum, String statFilePath, ConcurrentLinkedQueue<String> reportQueue,
      AbstractWorkload workload) throws IOException {
    this.tableName = tableName;
    this.processId = processId;
    this.threadNum = threadNum;
    this.loadDataDir = loadDataDir;
    this.statFilePath = statFilePath;
    this.reportQueue = reportQueue;
    this.conf = conf;
    loaders = new RunnableDataLoader[threadNum];
    inserters = new RunnablePerfInserter[threadNum];
    threadFinishMark = new boolean[threadNum];
    threadLatency = new double[threadNum];
    globalBoxNumber = new int[ResultParser.LatencyBoxPivots.length];
    for (int i = 0; i < globalBoxNumber.length; ++i) {
      globalBoxNumber[i] = 0;
    }
    this.workload = workload;
  }

  abstract protected void checkTable(byte[][] splits) throws IOException;

  public void loadAndInsertData() throws InterruptedException, IOException {
    for (int i = 0; i < threadNum; ++i) {
      threadFinishMark[i] = false;
      ConcurrentLinkedQueue<AbstractDITBRecord> queue = new ConcurrentLinkedQueue<>();
      loaders[i] = new RunnableDataLoader(i, reportInterval,
          DITBUtil.getDataFileName(loadDataDir, processId, i), queue);
      inserters[i] = getProperDataInserter(i, reportInterval, queue, finishCounter);
      new Thread(loaders[i]).start();
      new Thread(inserters[i]).start();
    }
  }

  public boolean hasFinished() {
    return finishCounter.getCounter() == threadNum;
  }

  abstract protected RunnablePerfInserter getProperDataInserter(int id, int reportInterval,
      ConcurrentLinkedQueue<AbstractDITBRecord> queue, FinishCounter fc) throws IOException;

  // 1st
  public double calAvgLatency() {
    double d = 0;
    for (int i = 0; i < threadNum; ++i) {
      d += threadLatency[i];
    }
    return d / threadNum;
  }

  // 2nd
  public double getMaxLatency() {
    return maxLatency.get() / 1000.0;
  }

  // 3rd
  public List<String> calDetailLatency() {
    List<String> latencyString = new ArrayList<String>();
    String prefix = "latency report ";
    int part = 0;
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < globalBoxNumber.length; ++i) {
      if (sb.length() > 500) {
        latencyString.add(sb.toString());
        sb = new StringBuilder();
        ++part;
      }
      if (sb.length() == 0) {
        sb.append(prefix).append("part: ").append(part);
      }
      sb.append(", [").append(ResultParser.LatencyBoxPivots[i]).append("->")
          .append(String.format(PERCENT_FORMAT, 100.0 * globalBoxNumber[i] / totalDoneSize))
          .append("]");
    }
    latencyString.add(sb.toString());
    return latencyString;
  }

  private long updateMaxLatency(long value) {
    long ret = 0;
    synchronized (maxLatency) {
      ret = maxLatency.get();
      if (ret < value) {
        maxLatency.set(value);
        ret = value;
      }
    }
    return ret;
  }

  public class RunnableDataLoader implements Runnable {
    protected final String dataFileName;
    protected final int PRINT_INTERVAL;
    protected final int id;
    protected final int LOAD_FILE_SLEEP_INTER_VAL = 5 * 1000;
    protected ConcurrentLinkedQueue<AbstractDITBRecord> queue;
    private final int MAX_QUEUE_SIZE = 2000;
    private final int BACK_INSERT_QUEUE_SIZE = 1000;
    private final Random random = new Random();

    // status from LOAD_DATA to LOAD_DATA_DONE
    public RunnableDataLoader(int id, int reportInterval, String fileName,
        ConcurrentLinkedQueue<AbstractDITBRecord> queue) {
      this.id = id;
      dataFileName = fileName;
      PRINT_INTERVAL = reportInterval;
      this.queue = queue;
    }

    @Override public void run() {
      try {
        try {
          loadData();
        } catch (ParseException e) {
          e.printStackTrace();
        }
        threadFinishMark[id] = true;
      } catch (IOException e) {
        e.printStackTrace();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    private void loadData() throws IOException, InterruptedException, ParseException {
      printAndAddtoReportQueue("coffey thread " + id + " load data from file: " + dataFileName);
      BufferedReader br = new BufferedReader(new FileReader(dataFileName));
      String line;
      int counter = 0;
      while ((line = br.readLine()) != null) {
        AbstractDITBRecord record = workload.loadDITBRecord(line);
        queue.add(record);
        ++counter;
        if (counter % PRINT_INTERVAL == 0) {
          printAndAddtoReportQueue(
              "coffey thread " + id + " load data into queue size: " + counter + " class: " + this
                  .getClass().getName());
        }
        if (queue.size() > MAX_QUEUE_SIZE) {
          while (queue.size() > BACK_INSERT_QUEUE_SIZE) {
            Thread.sleep(LOAD_FILE_SLEEP_INTER_VAL);
          }
        }
      }
      br.close();
      printAndAddtoReportQueue("coffey thread " + id + " totally load " + counter + " records");
    }
  }

  abstract public class RunnablePerfInserter implements Runnable {
    protected long totalLatency = 0;
    protected long innerMaxLatency = 0;
    protected int[] latencyBoxNumbers;
    protected final boolean CAL_LATENCY = true;
    ConcurrentLinkedQueue<AbstractDITBRecord> queue;
    protected final int PRINT_INTERVAL;
    protected final int id;
    protected final int SLEEP_INTERVAL = 100;
    protected int doneSize = 0;
    FinishCounter fc;

    // status from LOAD_DATA to INSERT_DATA_DONE
    public RunnablePerfInserter(int id, int reportInterval,
        ConcurrentLinkedQueue<AbstractDITBRecord> queue, FinishCounter fc) {
      this.id = id;
      PRINT_INTERVAL = reportInterval;
      this.queue = queue;
      this.fc = fc;
      latencyBoxNumbers = new int[ResultParser.LatencyBoxPivots.length];
      for (int i = 0; i < latencyBoxNumbers.length; ++i) {
        latencyBoxNumbers[i] = 0;
      }
    }

    @Override public void run() {
      try {
        insertData();
        threadLatency[id] = this.getLatency();
        System.out.println("coffey thread " + id + " before sync update data");
        synchronized (syncBoxObj) {
          for (int i = 0; i < latencyBoxNumbers.length; ++i) {
            globalBoxNumber[i] += latencyBoxNumbers[i];
          }
        }
        System.out.println("coffey thread " + id + " before sync update data");
      } catch (IOException e) {
        e.printStackTrace();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      fc.addCounterOne();
      printAndAddtoReportQueue(
          "coffey thread " + id + " finish, now fc is: " + fc.getCounter() + " total want: "
              + threadNum);
    }

    public void insertData() throws IOException, InterruptedException {
      HTable table = new HTable(conf, tableName);
      DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
      int counter = 0;
      long start = 0;
      while (true) {
        if (queue.isEmpty()) {
          if (threadFinishMark[id]) {
            break;
          } else {
            Thread.sleep(SLEEP_INTERVAL);
            continue;
          }
        }
        if (CAL_LATENCY) {
          start = System.currentTimeMillis();
        }
        insertOneRecord(queue.poll());
        if (CAL_LATENCY) {
          updateLatency(System.currentTimeMillis() - start);
        }
        if (counter == PRINT_INTERVAL) {
          counter = 0;
          printAndAddtoReportQueue(
              "coffey thread " + id + " insert data " + doneSize + " class: " + this.getClass()
                  .getName() + ", time: " + dateFormat.format(new Date()));
        }
        ++counter;
        ++doneSize;
      }
      table.close();
      printAndAddtoReportQueue("coffey totally insert " + doneSize + " records");
      synchronized (syncBoxObj) {
        totalDoneSize += doneSize;
      }
    }

    protected abstract void insertOneRecord(AbstractDITBRecord record) throws IOException;

    protected double getLatency() {
      printAndAddtoReportQueue(
          "coffey thread " + id + " latency " + totalLatency * 1.0 / doneSize / 1000);
      return totalLatency * 1.0 / doneSize / 1000;
    }

    protected void updateLatency(long timeInMS) {
      totalLatency += timeInMS;
      if (timeInMS > innerMaxLatency) {
        innerMaxLatency = updateMaxLatency(timeInMS);
      }
      double latency = timeInMS / 1000.0;
      //      for (int i = 0; i < ResultParser.LatencyBoxPivots.length; ++i) {
      //        if (ResultParser.LatencyBoxPivots[i] <= (timeInMS / 1000.0)) {
      //          ++latencyBoxNumbers[i];
      //          break;
      //        }
      //      }
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
  }

  protected synchronized void printAndAddtoReportQueue(String msg) {
    System.out.println(msg);
    reportQueue.add(msg);
  }

  public static class FinishCounter {
    private AtomicInteger counter;

    public FinishCounter(int init) {
      counter = new AtomicInteger(init);
    }

    public synchronized int getCounter() {
      return counter.get();
    }

    public synchronized void addCounterOne() {
      counter.incrementAndGet();
    }
  }
}
