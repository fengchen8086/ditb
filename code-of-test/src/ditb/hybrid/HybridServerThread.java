package ditb.hybrid;

import ditb.util.DITBConstants;
import ditb.util.DITBUtil;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * Created by winter on 17-1-10.
 */
public class HybridServerThread extends Thread {
  private final int innerBufferLength = 2048;
  private final int SLEEP_TIME = 100;
  private static final DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

  private final Socket sock;
  private final OutputStream sockOut;
  private final InputStream sockIn;

  private final String clientProcessId;
  private final String clientName;
  private final int processId;
  private final List<String> unFinishedClients;
  private final String dataDir;
  private final String param;
  private DITBConstants.RemotePutStatus status;
  private int threadNum;
  private String statFilePath;
  private Object syncObj;

  public HybridServerThread(Socket sock, List<String> unFinishedClients, String dataDir, String param,
      int threadNum, String statFileName, Object syncObj) throws IOException {
    this.sock = sock;
    status = DITBConstants.RemotePutStatus.SAY_HI;
    sockOut = sock.getOutputStream();
    sockIn = sock.getInputStream();
    clientProcessId = readShortMessage();
    clientName = DITBUtil.getClientName(clientProcessId);
    processId = DITBUtil.getProcessId(clientProcessId);
    this.syncObj = syncObj;
    this.threadNum = threadNum;
    System.out.println(
        "the remote process is: " + getClientProcessId() + ", want " + threadNum + " threads");
    this.dataDir = dataDir;
    this.param = param;
    this.statFilePath = dataDir + "/" + statFileName;
    this.unFinishedClients = unFinishedClients;
  }

  private void deinit() throws IOException {
    if (sockOut != null) sockOut.close();
    if (sockIn != null) sockIn.close();
    if (sock != null) sock.close();
  }

  private void setCurrentFinished() {
    long start = System.currentTimeMillis();
    synchronized (syncObj) {
      if (unFinishedClients.contains(getClientProcessId())) {
        unFinishedClients.remove(getClientProcessId());
      }
    }
    System.out.println(
        "server thread spend " + (System.currentTimeMillis() - start) / 1000.0 + " seconds to mark "
            + getClientProcessId() + " free for status " + status);
    this.status = DITBConstants.RemotePutStatus.SAY_HI;
  }

  private String readShortMessage() throws IOException {
    byte[] bufName = new byte[innerBufferLength];
    int lenInfo = sockIn.read(bufName);
    return new String(bufName, 0, lenInfo);
  }

  @Override public void run() {
    try {
      while (status != DITBConstants.RemotePutStatus.CLOSE) {
        switch (status) {
        // 1. send param
        case SEND_PARAM:
          sockOut.write(Bytes.toBytes(String.valueOf(DITBConstants.RemotePutStatus.SEND_PARAM)));
          readShortMessage();
          sockOut.write(Bytes.toBytes(param));
          readShortMessage();
          setCurrentFinished();
          break;
        // 2. check client data
        case CHECK_DATA:
          sockOut.write(Bytes.toBytes(String.valueOf(DITBConstants.RemotePutStatus.CHECK_DATA)));
          boolean ready = Boolean.valueOf(readShortMessage());
          if (!ready) {
            System.out.println(
                "remote client data not ready, transfer file now: " + getClientProcessId());
            transferFile();
          }
          setCurrentFinished();
          break;
        // 3. check table
        case CHECK_TABLE:
          sockOut.write(Bytes.toBytes(String.valueOf(DITBConstants.RemotePutStatus.CHECK_TABLE)));
          readShortMessage();
          setCurrentFinished();
          break; // comment it?
        // 4. start put
        case START_TO_WORK:
          sockOut.write(Bytes.toBytes(String.valueOf(DITBConstants.RemotePutStatus.START_TO_WORK)));
          while (true) {
            String msg = readShortMessage();
            if (DITBConstants.REMOTE_PUT_DONE_MSG.equals(msg)) { // ends with an read
              setCurrentFinished();
              break;
            } else if (DITBConstants.REMOTE_PUTTING_MSG.equals(msg)) {
              writeNoMeanMessage();
              Thread.sleep(SLEEP_TIME * 1);
            } else if (DITBConstants.REMOTE_PUTTING_REPORT_MSG.equals(msg)) {
              writeNoMeanMessage();
              msg = readShortMessage();
              writeNoMeanMessage();
              System.out.println(
                  "[" + dateFormat.format(new Date()) + "] from " + getClientProcessId()
                      + ": " + msg);
            } else {
              throw new IOException("meet unknown message in putting: " + msg);
            }
          }
          break;
        // 5. flush
        case FLUSH:
          sockOut.write(Bytes.toBytes(String.valueOf(DITBConstants.RemotePutStatus.FLUSH)));
          readShortMessage();
          setCurrentFinished();
          break;
        // 6. otherwise, it will be set to say hi
        case SAY_HI:
        default:
          sockOut.write(Bytes.toBytes(String.valueOf(DITBConstants.RemotePutStatus.SAY_HI)));
          readShortMessage();
          Thread.sleep(SLEEP_TIME);
        }
      }
      // 7. now close
      sockOut.write(Bytes.toBytes(String.valueOf(DITBConstants.RemotePutStatus.CLOSE)));
      readReportsAndPrint();
      setCurrentFinished();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      try {
        deinit();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  private void readReportsAndPrint() throws IOException {
    // write report
    String msg = readShortMessage();
    System.out.println("report " + getClientProcessId() + ": " + msg);
    writeNoMeanMessage();
    // read report
    msg = readShortMessage();
    System.out.println("report " + getClientProcessId() + ": " + msg);
    writeNoMeanMessage();
    // scan report
    int times = Integer.valueOf(readShortMessage());
    writeNoMeanMessage();
    while (times > 0) {
      System.out.println("report " + getClientProcessId() + ": " + readShortMessage());
      writeNoMeanMessage();
      --times;
    }
    // write latency detail
    times = Integer.valueOf(readShortMessage());
    writeNoMeanMessage();
    while (times > 0) {
      System.out.println("report " + getClientProcessId() + ": " + readShortMessage());
      writeNoMeanMessage();
      --times;
    }
    // read latency detail
    times = Integer.valueOf(readShortMessage());
    writeNoMeanMessage();
    while (times > 0) {
      System.out.println("report " + getClientProcessId() + ": " + readShortMessage());
      writeNoMeanMessage();
      --times;
    }
    readShortMessage();
    System.out.println("client " + getClientProcessId() + " close");
  }

  /**
   * send file one by one to client
   *
   * @throws IOException
   */
  private void transferFile() throws IOException {
    String clientDir = DITBUtil.getChildPath(dataDir, clientName);
    for (int i = 0; i < threadNum; ++i) {
      String wantedFile = DITBUtil.getDataFileName(clientDir, processId, i);
      sendOneFile(wantedFile);
    }
    sendOneFile(statFilePath);
  }

  /**
   * send file by tcp
   *
   * @param fileName
   * @throws IOException
   */
  private void sendOneFile(String fileName) throws IOException {
    byte[] buffer = new byte[DITBConstants.REMOTE_FILE_TRANS_BUFFER_LEN];
    File file = new File(fileName);
    FileInputStream fis = new FileInputStream(file); // read local file
    int len = 0;
    sockOut.write(Bytes.toBytes(String.valueOf(file.length())));
    readShortMessage();
    while (true) {
      len = fis.read(buffer);
      if (len > 0) {
        sockOut.write(buffer, 0, len); // write data!
      } else {
        break;
      }
    }
    fis.close();
    readShortMessage();
  }

  private void writeNoMeanMessage() throws IOException {
    writeNoMeanMessage(false);
  }

  private void writeNoMeanMessage(boolean flush) throws IOException {
    sockOut.write(DITBConstants.NO_MEANS_BYTES);
    if (flush) sockOut.flush();
  }

  public void setStatus(DITBConstants.RemotePutStatus status) {
    this.status = status;
  }

  public String getClientProcessId() {
    return clientProcessId;
  }
}
