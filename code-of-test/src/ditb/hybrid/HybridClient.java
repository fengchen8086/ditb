package ditb.hybrid;

import ditb.util.DITBConstants;
import ditb.util.DITBUtil;
import ditb.workload.AbstractWorkload;
import org.apache.hadoop.hbase.index.IndexType;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

public class HybridClient {

  private int PORT = DITBConstants.REMOTE_SERVER_PORT;
  private Socket sock;
  private OutputStream sockOut;
  private InputStream sockIn;

  private final int innerBufferLength = 2048;
  private final int SLEEP_TIME = 100;

  private String dataDir;
  private String loadDataDir;
  private String clientName;
  private String clientProcessDesc;
  private String statFilePath;
  private HybridWorker worker;
  private IndexType indexType;
  private int processId;
  private int threadNum;
  private ConcurrentLinkedQueue<String> reportQueue;
  private AbstractWorkload workload;

  /**
   * working!
   *
   * @param serverName   hostname of server
   * @param thisHost     current hostname
   * @param threadNumber number of thread on client side;
   * @throws IOException
   * @throws InterruptedException
   */
  public void work(String serverName, String thisHost, int processId, int threadNumber)
      throws IOException, InterruptedException {
    System.out.println("client connecting to " + serverName + ":" + PORT);
    long start = System.currentTimeMillis();
    while (sock == null) {
      try {
        sock = new Socket(serverName, PORT);
      } catch (Exception e) {
        sock = null;
        Thread.sleep(1000);
      }
      if ((System.currentTimeMillis() - start) > 60 * 1000) {
        break;
      }
    }
    if (sock == null) {
      throw new IOException(
          "oh oh, client " + thisHost + " cannot connect to " + serverName + ":" + PORT
              + "after trying 60 seconds");
    }
    System.out.println(
        "client connect to " + serverName + ":" + PORT + " success, this is: " + thisHost + " for "
            + threadNumber + " threads");
    sockOut = sock.getOutputStream();
    sockIn = sock.getInputStream();
    this.clientName = thisHost;
    this.threadNum = threadNumber;
    this.processId = processId;
    this.clientProcessDesc = DITBUtil.toClientProcessId(clientName, processId);
    reportQueue = new ConcurrentLinkedQueue<>();
    sockOut.write(Bytes.toBytes(clientProcessDesc));
    // init done, do main work
    mainLoop();
    // all done, exit
    System.out.println("client " + getClientProcessDesc() + " finish all threads, return");
  }

  /**
   * main loop
   *
   * @throws IOException
   * @throws InterruptedException
   */
  private void mainLoop() throws IOException, InterruptedException {
    while (true) {
      DITBConstants.RemotePutStatus status =
          DITBConstants.RemotePutStatus.valueOf(readShortMessage());
      if (status != DITBConstants.RemotePutStatus.SAY_HI) {
        System.out.println("client " + getClientProcessDesc() + " receive new status: " + status);
      }
      // 0. if closing, de-init then exit
      if (status == DITBConstants.RemotePutStatus.CLOSE) {
        sendReports();
        deinit();
        break;
      }
      // otherwise switch
      switch (status) {
      // 1. server sending param, client receive and parse parameters.
      case SEND_PARAM:
        System.out.println("client " + getClientProcessDesc() + " " + status + " doing");
        writeNoMeanMessage();
        parseParams(readShortMessage());
        writeNoMeanMessage();
        System.out.println("client " + getClientProcessDesc() + " " + status + " done");
        break;
      // 2. checking data, if not ready, ask and receive file from server
      case CHECK_DATA:
        boolean ready = inputFilesReady();
        sockOut.write(Bytes.toBytes(String.valueOf(ready)));
        if (!ready) {
          System.out
              .println("client " + getClientProcessDesc() + " need to transfer data from server");
          receiveFiles();
        }
        System.out.println("client " + getClientProcessDesc() + " " + status + " done");
        break;
      // 3. check table, only a single client will do this
      case CHECK_TABLE:
        if (worker == null) {
          worker = createWorker();
        }
        worker.checkTable(workload.getSplits());
        writeNoMeanMessage();
        System.out.println("client " + getClientProcessDesc() + " " + status + " done");
        break;
      // 4. start to put!
      case START_TO_WORK:
        if (worker == null) {
          worker = createWorker();
        }
        worker.loadAndExecuteOperations();
        while (true) {
          while (reportQueue.size() > 0) { // admin reaches threshold, reports msg to server
            sockOut.write(Bytes.toBytes(DITBConstants.REMOTE_PUTTING_REPORT_MSG));
            readShortMessage();
            sockOut.write(Bytes.toBytes(reportQueue.poll()));
            readShortMessage();
          }
          if (worker.hasFinished()) { // break, ends with an write
            System.out.println("client " + getClientProcessDesc() + " " + status + " done1");
            sockOut.write(Bytes.toBytes(DITBConstants.REMOTE_PUT_DONE_MSG));
            System.out.println("client " + getClientProcessDesc() + " " + status + " done2");
            break;
          } else { // still doing, heart beat
            sockOut.write(Bytes.toBytes(DITBConstants.REMOTE_PUTTING_MSG));
            readShortMessage();
            Thread.sleep(SLEEP_TIME * 2);
          }
        }
        break;
      // 5. flush table, do nothing in current version
      case FLUSH:
        writeNoMeanMessage();
        break;
      // 6. just heart beat, write blank message back to co-ordinate
      case SAY_HI:
      default:
        writeNoMeanMessage(true);
        Thread.sleep(SLEEP_TIME);
      }
    }
  }

  private void sendReports() throws IOException {
    LatencyStatistics.WRSLatency finalLatency =
        LatencyStatistics.mergeWRSLatencies(worker.threadLatencies);
    sockOut.write(Bytes.toBytes(String.format("[write latency, avg = %.7f, max=%.7f]",
        finalLatency.writeStatistics.getAvergeLatency(),
        finalLatency.writeStatistics.getMaxLatency())));
    readShortMessage();
    sockOut.write(Bytes.toBytes(String.format("[read latency, avg = %.7f, max=%.7f]",
        finalLatency.readStatistics.getAvergeLatency(),
        finalLatency.readStatistics.getMaxLatency())));
    readShortMessage();
    sockOut.write(Bytes.toBytes(String.valueOf(finalLatency.scanCounts.size())));
    readShortMessage();
    for (Map.Entry<String, Long> entry : finalLatency.scanCounts.entrySet()) {
      sockOut.write(Bytes.toBytes(String
          .format("[scan time for %s, avg = %.2f for %d times]", entry.getKey(),
              finalLatency.scanTimes.get(entry.getKey()) / 1000.0 / entry.getValue(),
              entry.getValue())));
      readShortMessage();
    }
    // write latency detail
    List<String> list = finalLatency.writeStatistics.calDetailLatency("write latency detail");
    sockOut.write(Bytes.toBytes(String.valueOf(list.size())));
    readShortMessage();
    for (String str : list) {
      sockOut.write(Bytes.toBytes(str));
      readShortMessage();
    }
    list = finalLatency.readStatistics.calDetailLatency("read latency detail");
    sockOut.write(Bytes.toBytes(String.valueOf(list.size())));
    readShortMessage();
    for (String str : list) {
      sockOut.write(Bytes.toBytes(str));
      readShortMessage();
    }
    writeNoMeanMessage();
  }

  /**
   * TODO
   * parse putter based on selected class path, should be replace with new IndexTableAdmin
   *
   * @return
   * @throws IOException
   */
  private HybridWorker createWorker() throws IOException {
    if (indexType == IndexType.MDIndex) {
      return new HybridMDWorker(workload.getHBaseConfiguration(), workload.getTableName(),
          loadDataDir, processId, threadNum, statFilePath, reportQueue, indexType, workload);
    } else {
      return new HybridNormalWorker(workload.getHBaseConfiguration(), workload.getTableName(),
          loadDataDir, processId, threadNum, statFilePath, reportQueue, indexType, workload);
    }
  }

  /**
   * parse parameters from the new read line
   *
   * @param paramLine
   * @throws IOException
   */
  private void parseParams(String paramLine) throws IOException {
    System.out.println("client " + getClientProcessDesc() + " receive paramLine: " + paramLine);
    String splits[] = paramLine.split(DITBConstants.DELIMITER);
    dataDir = splits[0];
    indexType = IndexType.valueOf(splits[1]);
    statFilePath = dataDir + "/" + splits[2];
    loadDataDir = DITBUtil.getChildPath(dataDir, clientName);
    clientProcessDesc = clientProcessDesc + "@" + indexType;
    workload = AbstractWorkload.getWorkload(splits[3], splits[4]);
  }

  /**
   * receive files from server
   *
   * @throws IOException
   */
  private void receiveFiles() throws IOException {
    for (int i = 0; i < threadNum; ++i) {
      String str = DITBUtil.getDataFileName(loadDataDir, processId, i);
      receiveFile(str);
    }
    receiveFile(statFilePath);
  }

  /**
   * receive file from remote, write to filename
   *
   * @param fileName target filename
   * @throws IOException
   */
  private void receiveFile(String fileName) throws IOException {
    System.out.println("now receive file " + fileName + " from server");
    byte[] buffer = new byte[DITBConstants.REMOTE_FILE_TRANS_BUFFER_LEN];
    File file = new File(fileName);
    if (!file.getParentFile().exists()) {
      file.getParentFile().mkdirs();
    }
    if (file.exists()) {
      file.delete();
    }
    file.createNewFile();
    FileOutputStream fos = new FileOutputStream(file);
    String msg = readShortMessage();
    long totalLength = Long.valueOf(msg);
    writeNoMeanMessage();
    int receivedCount = 0;
    while (true) {
      int len = sockIn.read(buffer);
      if (len > 0) {
        fos.write(buffer, 0, len);
      }
      receivedCount += len;
      if (len < 0 || receivedCount == totalLength) {
        break;
      }
    }
    fos.close();
    writeNoMeanMessage();
  }

  /**
   * check input files (including data file and stat file) ready
   *
   * @return
   */
  private boolean inputFilesReady() {
    for (int i = 0; i < threadNum; ++i) {
      String str = DITBUtil.getDataFileName(loadDataDir, processId, i);
      if (!new File(str).exists()) {
        return false;
      }
    }
    return new File(statFilePath).exists();
  }

  /**
   * close socks
   *
   * @throws IOException
   */
  private void deinit() throws IOException {
    if (worker != null) worker.close();
    if (sockOut != null) sockOut.close();
    if (sockIn != null) sockIn.close();
    if (sock != null) sock.close();
  }

  private void writeNoMeanMessage() throws IOException {
    writeNoMeanMessage(false);
  }

  /**
   * write a blank message, to co-ordinate server-client operations
   *
   * @param flush force flush
   * @throws IOException
   */
  private void writeNoMeanMessage(boolean flush) throws IOException {
    sockOut.write(DITBConstants.NO_MEANS_BYTES);
    if (flush) sockOut.flush();
  }

  private String getClientProcessDesc() {
    return clientProcessDesc;
  }

  /**
   * read short message from server
   *
   * @return
   * @throws IOException
   */
  private String readShortMessage() throws IOException {
    byte[] bufName = new byte[innerBufferLength];
    int lenInfo = 0;
    lenInfo = sockIn.read(bufName);
    return new String(bufName, 0, lenInfo);
  }

  public static void usage() {
    System.out.println("DITBClient serverName thisHost processId threadNumber");
  }

  public static void main(String[] args)
      throws NumberFormatException, IOException, InterruptedException {
    System.out.println("DITBClient runs!");
    if (args.length < 4) {
      usage();
      System.out.println("current:");
      for (String str : args) {
        System.out.println(str);
      }
      return;
    }
    new HybridClient().work(args[0], args[1], Integer.valueOf(args[2]), Integer.valueOf(args[3]));
  }
}
