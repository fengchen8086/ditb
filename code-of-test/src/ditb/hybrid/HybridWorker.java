package ditb.hybrid;

import ditb.util.DITBUtil;
import ditb.workload.AbstractWorkload;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class HybridWorker {

  // hbase operation
  protected final TableName tableName;
  protected final Configuration conf;

  // multiple thread
  final int threadNum;
  final String loadDataDir;
  final String statFilePath;
  boolean[] threadFinishMark;
  LatencyStatistics.WRSLatency[] threadLatencies;
  final int reportInterval = 10000;
  final Queue<String> reportQueue;
  FinishCounter finishCounter = new FinishCounter(0);
  OperationLoader[] loaders;
  OperationExecutor[] executors;
  int processId;
  AbstractWorkload workload;

  public HybridWorker(Configuration conf, TableName tableName, String loadDataDir, int processId,
      int threadNum, String statFilePath, ConcurrentLinkedQueue<String> reportQueue,
      AbstractWorkload workload) throws IOException {
    this.tableName = tableName;
    this.processId = processId;
    this.threadNum = threadNum;
    this.loadDataDir = loadDataDir;
    this.statFilePath = statFilePath;
    this.reportQueue = reportQueue;
    this.conf = conf;
    loaders = new OperationLoader[threadNum];
    executors = new OperationExecutor[threadNum];
    threadFinishMark = new boolean[threadNum];
    threadLatencies = new LatencyStatistics.WRSLatency[threadNum];
    this.workload = workload;
  }

  protected abstract void checkTable(byte[][] splits) throws IOException;

  public abstract void close() throws IOException;

  public void loadAndExecuteOperations() throws InterruptedException, IOException {
    for (int i = 0; i < threadNum; ++i) {
      threadFinishMark[i] = false;
      ConcurrentLinkedQueue<Operation> queue = new ConcurrentLinkedQueue<>();
      loaders[i] = new OperationLoader(i, reportInterval,
          DITBUtil.getDataFileName(loadDataDir, processId, i), queue);
      executors[i] = getOperationExecutor(i, reportInterval, queue, finishCounter);
      new Thread(loaders[i]).start();
      new Thread(executors[i]).start();
    }
  }

  public boolean hasFinished() {
    return finishCounter.getCounter() == threadNum;
  }

  abstract protected OperationExecutor getOperationExecutor(int id, int reportInterval,
      ConcurrentLinkedQueue<Operation> queue, FinishCounter fc) throws IOException;

  public class OperationLoader implements Runnable {
    protected final String dataFileName;
    protected final int PRINT_INTERVAL;
    protected final int id;
    protected final int LOAD_FILE_SLEEP_INTER_VAL = 5 * 1000;
    protected ConcurrentLinkedQueue<Operation> queue;
    private final int MAX_QUEUE_SIZE = 2000;
    private final int BACK_INSERT_QUEUE_SIZE = 1000;

    // status from LOAD_DATA to LOAD_DATA_DONE
    public OperationLoader(int id, int reportInterval, String fileName,
        ConcurrentLinkedQueue<Operation> queue) {
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
      printAndAddtoReportQueue("thread " + id + " load operations from file: " + dataFileName);
      BufferedReader br = new BufferedReader(new FileReader(dataFileName));
      String line;
      int counter = 0;
      while ((line = br.readLine()) != null) {
        Operation operation = Operation.parseOperation(line, workload);
        queue.add(operation);
        ++counter;
        if (counter % PRINT_INTERVAL == 0) {
          printAndAddtoReportQueue(
              "thread " + id + " load operations into queue size: " + counter + " class: " + this
                  .getClass().getName());
        }
        if (queue.size() > MAX_QUEUE_SIZE) {
          while (queue.size() > BACK_INSERT_QUEUE_SIZE) {
            Thread.sleep(LOAD_FILE_SLEEP_INTER_VAL);
          }
        }
      }
      br.close();
      printAndAddtoReportQueue("thread " + id + " totally load " + counter + " operations");
    }
  }

  abstract public class OperationExecutor implements Runnable {

    LatencyStatistics.WRSLatency wrsLatency;
    ConcurrentLinkedQueue<Operation> queue;
    protected final int PRINT_INTERVAL;
    protected final int id;
    protected final int SLEEP_INTERVAL = 100;
    protected int doneSize = 0;
    FinishCounter fc;

    public OperationExecutor(int id, int reportInterval, ConcurrentLinkedQueue<Operation> queue,
        FinishCounter fc) {
      this.id = id;
      PRINT_INTERVAL = reportInterval;
      this.queue = queue;
      this.fc = fc;
      wrsLatency = new LatencyStatistics.WRSLatency();
    }

    @Override public void run() {
      try {
        executeOperations();
        threadLatencies[id] = this.wrsLatency;
      } catch (IOException e) {
        e.printStackTrace();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      fc.addCounterOne();
      printAndAddtoReportQueue(
          "thread " + id + " finish, now fc is: " + fc.getCounter() + " total want: " + threadNum);
    }

    public void executeOperations() throws IOException, InterruptedException {
      DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
      int counter = 0;
      while (true) {
        if (queue.isEmpty()) {
          if (threadFinishMark[id]) {
            break;
          } else {
            Thread.sleep(SLEEP_INTERVAL);
            continue;
          }
        }
        long start = System.currentTimeMillis();
        Operation operation = queue.poll();
        executeOperation(operation);
        wrsLatency.updateLatency(operation, System.currentTimeMillis() - start);
        if (counter == PRINT_INTERVAL) {
          counter = 0;
          printAndAddtoReportQueue(
              "thread " + id + " executed " + doneSize + " operation, class: " + this.getClass()
                  .getName() + ", time: " + dateFormat.format(new Date()));
        }
        ++counter;
        ++doneSize;
      }
      printAndAddtoReportQueue("totally executed " + doneSize + " operations");
    }

    protected void executeOperation(Operation operation) throws IOException {
      if (operation.isWrite()) {
        executeWrite(operation);
      } else if (operation.isRead()) {
        executeRead(operation);
      } else if (operation.isScan()) {
        int resultCount = executeScan(operation);
        System.out.println(String.format("have %d results for scan %s, filter=%s", resultCount,
            operation.getScan().getId(), operation.getScan().getFilter().toString()));
      }
    }

    protected abstract void executeWrite(Operation operation) throws IOException;

    protected abstract void executeRead(Operation operation) throws IOException;

    protected abstract int executeScan(Operation operation) throws IOException;
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
