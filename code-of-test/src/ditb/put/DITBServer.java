package ditb.put;

import ditb.util.DITBConstants;
import ditb.util.DITBUtil;
import org.apache.hadoop.hbase.index.IndexType;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by winter on 17-1-10.
 */
public class DITBServer {

  private final int SERVER_PORT = DITBConstants.REMOTE_SERVER_PORT;
  private List<String> clients;
  private List<String> unFinishedClients;
  private List<String> remoteProcesses;
  private List<DITBServerThread> threads;

  private final int ReportInterval = 100;
  private final int SLEEP_TIME = 100;

  /**
   * @param indexType      indexType
   * @param dataDir        input source dir
   * @param statFileName   stat file name
   * @param clientFileName file holding client names
   * @param nbThread       the number of thread on each client node
   * @throws IOException
   * @throws InterruptedException
   */
  private void work(IndexType indexType, String dataDir, String statFileName, String clientFileName,
      int nbProcess, int nbThread, String workloadClassName, String workloadDescFile)
      throws IOException, InterruptedException {
    clients = DITBUtil.getClientHosts(clientFileName);
    remoteProcesses = new ArrayList<>();
    for (String host : clients) {
      for (int i = 0; i < nbProcess; i++) {
        remoteProcesses.add(DITBUtil.toClientProcessId(host, i));
      }
    }
    unFinishedClients = new ArrayList<>();
    // start server socket
    resetUnFinishedClients();
    threads = new ArrayList<>();
    // host name and id is given at start
    String param =
        dataDir + DITBConstants.DELIMITER + indexType + DITBConstants.DELIMITER + statFileName
            + DITBConstants.DELIMITER + workloadClassName + DITBConstants.DELIMITER
            + workloadDescFile;
    List<String> unConnectedClients = new ArrayList<>();
    unConnectedClients.addAll(remoteProcesses);
    ServerSocket ss = new ServerSocket(SERVER_PORT);
    System.out.println("server " + indexType + " param: " + param);
    String desc =
        clients.size() + " clients with " + nbProcess + " processes and " + nbThread + " threads";
    Object syncObj = new Object();
    do {
      Socket s = ss.accept();
      DITBServerThread th =
          new DITBServerThread(s, unFinishedClients, dataDir, param, nbThread, statFileName,
              syncObj);
      threads.add(th);
      unConnectedClients.remove(th.getClientProcessId());
      System.out.println(
          "see connection from " + th.getClientProcessId() + ", left size: " + unConnectedClients
              .size());
      th.start();
    } while (unConnectedClients.size() > 0);
    // init connection done
    // start and send parameters to client
    System.out.println("server " + indexType + " send params now");
    sendParam();
    System.out.println("server " + indexType + " check data ready now");
    checkDataReady();
    System.out.println("server " + indexType + " check remote table");
    checkRemoteTable();
    System.out.println(
        "server " + indexType + " start remote put, on " + clients.size() + " clients * "
            + nbThread);
    long start = System.currentTimeMillis();
    startRemotePut();
    System.out.println("server " + indexType + " remote flush table");
    long flushStart = System.currentTimeMillis();
    System.out.println("report put insert " + indexType + ", cost " + (flushStart - start) / 1000.0
        + " to write on " + desc);
    setRemoteFlush();
    System.out.println("report put flush " + indexType + ", cost "
        + (System.currentTimeMillis() - flushStart) / 1000.0 + " to flush on " + desc);
    long totalTime = System.currentTimeMillis() - start;
    System.out.println("server " + indexType + " set remote exit");
    setRemoteStop();
    System.out.println("server " + indexType + " join threads, to wait for finish");
    for (DITBServerThread t : threads) {
      t.join();
    }
    ss.close();
    System.out.println("server " + indexType + " finish");
    System.out.println(
        "report put total " + indexType + ", cost " + (totalTime) / 1000.0 + " to write on "
            + desc);
  }

  /**
   * set status
   *
   * @param s
   */
  private void setAllThreadsStatus(DITBConstants.RemotePutStatus s) {
    for (DITBServerThread t : threads) {
      t.setStatus(s);
    }
  }

  /**
   * clear and reset unFinishedClients for later use
   */
  private void resetUnFinishedClients() {
    synchronized (unFinishedClients) {
      unFinishedClients.clear();
      unFinishedClients.addAll(remoteProcesses);
    }
  }

  /**
   * loop until all clients finish same operation
   *
   * @throws InterruptedException
   */
  private void waitClientsFinish() throws InterruptedException {
    int meetCount = 0;
    while (true) {
      synchronized (unFinishedClients) {
        if (unFinishedClients.size() == 0) {
          return;
        }
        if (unFinishedClients.size() < 3 && ++meetCount == ReportInterval) {
          meetCount = 0;
          System.out.println("unfinished clients: ");
          for (String s : unFinishedClients) {
            System.out.print(s + ",");
          }
          System.out.println("");
        }
        Thread.sleep(SLEEP_TIME);
      }
    }
  }

  /**
   * 1. send param to client
   *
   * @throws InterruptedException
   */
  private void sendParam() throws InterruptedException {
    resetUnFinishedClients();
    setAllThreadsStatus(DITBConstants.RemotePutStatus.SEND_PARAM);
    waitClientsFinish();
  }

  /**
   * 2. check data ready on client side
   *
   * @throws InterruptedException
   */
  private void checkDataReady() throws InterruptedException {
    resetUnFinishedClients();
    setAllThreadsStatus(DITBConstants.RemotePutStatus.CHECK_DATA);
    waitClientsFinish();
  }

  /**
   * 3. use the 1st client to check/recreate table
   *
   * @throws InterruptedException
   */
  private void checkRemoteTable() throws InterruptedException {
    String clientProcessId = threads.get(0).getClientProcessId();
    synchronized (unFinishedClients) {
      unFinishedClients.clear();
      unFinishedClients.add(clientProcessId);
    }
    System.out.println("checking table at: " + clientProcessId);
    threads.get(0).setStatus(DITBConstants.RemotePutStatus.CHECK_TABLE);
    waitClientsFinish();
  }

  /**
   * 4. start put
   *
   * @throws InterruptedException
   */
  private void startRemotePut() throws InterruptedException {
    resetUnFinishedClients();
    setAllThreadsStatus(DITBConstants.RemotePutStatus.START_TO_WORK);
    waitClientsFinish();
  }

  /**
   * 5. client flush
   *
   * @throws InterruptedException
   */
  private void setRemoteFlush() throws InterruptedException {
    String clientProcessId = threads.get(0).getClientProcessId();
    synchronized (unFinishedClients) {
      unFinishedClients.clear();
      unFinishedClients.add(clientProcessId);
    }
    threads.get(0).setStatus(DITBConstants.RemotePutStatus.FLUSH);
    waitClientsFinish();
  }

  /**
   * 6. client stop
   *
   * @throws InterruptedException
   */
  private void setRemoteStop() throws InterruptedException {
    resetUnFinishedClients();
    setAllThreadsStatus(DITBConstants.RemotePutStatus.CLOSE);
    waitClientsFinish();
  }

  public static void usage() {
    System.out.println(
        "DITBServer indexType dataDir statFileName clientHostFile noProcess noThread workloadClass workloadDescFile");
  }

  public static void main(String[] args)
      throws NumberFormatException, IOException, InterruptedException {
    if (args.length < 8) {
      usage();
      System.out.println("current: ");
      for (String str : args) {
        System.out.println(str);
      }
      return;
    }
    new DITBServer()
        .work(IndexType.valueOf(args[0]), args[1], args[2], args[3], Integer.valueOf(args[4]),
            Integer.valueOf(args[5]), args[6], args[7]);
  }
}
