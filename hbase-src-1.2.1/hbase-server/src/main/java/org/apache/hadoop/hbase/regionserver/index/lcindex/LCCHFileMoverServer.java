package org.apache.hadoop.hbase.regionserver.index.lcindex;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.index.jobs.BasicJobQueue;
import org.apache.hadoop.hbase.regionserver.index.jobs.CommitJobQueue;
import org.apache.hadoop.hbase.regionserver.index.jobs.CompleteCompactionJobQueue;
import org.apache.hadoop.hbase.util.Bytes;

public class LCCHFileMoverServer implements Runnable {

  private static final Log LOG = LogFactory.getLog(LCCHFileMoverServer.class);

  private int serverPort;
  private int bufferLength = 0;
  private AtomicInteger runningThreads = new AtomicInteger(0);
  private long totalSendSize = 0;
  private int totalSendFileNum = 0;

  public LCCHFileMoverServer(Configuration conf) throws IOException {
    serverPort =
        conf.getInt(LCIndexConstant.LCC_MOVER_PORT, LCIndexConstant.DEFAULT_LCC_MOVER_PORT);
    bufferLength = conf.getInt(LCIndexConstant.LCC_MOVER_BUFFER_LEN,
        LCIndexConstant.DEFAULT_LCC_MOVER_BUFFER_LEN);
  }

  private boolean keepAlive = true;

  @Override public void run() {
    ServerSocket ss = null;
    try {
      ss = new ServerSocket(serverPort);
      LOG.info("LCHFileMoverServer build on port: " + serverPort);
      while (keepAlive) {
        // should change to nio, otherwise will block here!
        // may be connecting localhost is possible
        Socket s = ss.accept();
        // System.out.println("winter LCCHFileMoverServer running thread number: "
        // + runningThreads.incrementAndGet());
        runningThreads.incrementAndGet();
        new ServerThread(s, bufferLength).start();
      }
      while (runningThreads.intValue() > 0) {
        LOG.info("winter sleeping for " + runningThreads.intValue() + " threads");
        Thread.sleep(5000);
      }
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      if (ss != null) {
        try {
          ss.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
    LOG.info("winter LCCHFileMoverServer deinit done");
  }

  public void setToClose() {
    synchronized (this) {
      keepAlive = false;
    }
  }

  class ServerThread extends Thread {
    private int shortBufferLen = 1024;
    private Socket sock = null;
    private OutputStream sockOut;
    InputStream sockIn;
    byte[] buffer;

    public ServerThread(Socket sock, int bufferLen) throws IOException {
      this.sock = sock;
      sockOut = sock.getOutputStream();
      sockIn = sock.getInputStream();
      buffer = new byte[bufferLen];
    }

    public void run() {
      try {
        work();
      } catch (IOException e) {
        LOG.error(e.getMessage());
        e.printStackTrace();
      } catch (NoSuchAlgorithmException e) {
        e.printStackTrace();
      } finally {
        try {
          runningThreads.decrementAndGet();
          deinit();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }

    private void deinit() throws IOException {
      if (sockOut != null) sockOut.close();
      if (sockIn != null) sockIn.close();
      if (sock != null) sock.close();
      buffer = null;
    }

    private String readShortMessage() throws IOException {
      return readShortMessage(null);
    }

    private String readShortMessage(String prevMessage) throws IOException {
      byte[] bufName = new byte[shortBufferLen];
      int lenInfo = 0;
      lenInfo = sockIn.read(bufName);
      if (lenInfo == -1 && prevMessage != null) {
        if (prevMessage.startsWith(LCIndexConstant.DELETE_HEAD_MSG)) {
          return prevMessage.substring(LCIndexConstant.DELETE_HEAD_MSG.length());
        } else if (prevMessage.startsWith(LCIndexConstant.REQUIRE_HEAD_MSG)) {
          return prevMessage.substring(LCIndexConstant.REQUIRE_HEAD_MSG.length());
        }
      }
      return new String(bufName, 0, lenInfo);
    }

    private void deleteFile(String targetName) throws IOException {
      File file = new File(targetName);
      if (file.exists()) {
        if (file.isDirectory()) {
          FileUtils.deleteDirectory(file);
        } else {
          file.delete();
        }
        sockOut.write(Bytes.toBytes(LCIndexConstant.DELETE_SUCCESS_MSG));
      } else {
        // actually, any thing is ok
        sockOut.write(Bytes.toBytes(LCIndexConstant.LCC_LOCAL_FILE_NOT_FOUND_MSG));
      }
    }

    private void transferFile(File file) throws IOException {
      FileInputStream fis = new FileInputStream(file); // read local file
      // send file to client
      int len = 0;
      long fileLong = file.length();
      totalSendSize += fileLong;
      ++totalSendFileNum;
      LOG.info(
          "winter LCCHFileMoverServer send a new file: " + file.getAbsolutePath() + ", length: "
              + String.valueOf(fileLong) + ", now the totalSendSize is " + totalSendSize + " for "
              + totalSendFileNum + " files");
      sockOut.write(Bytes.toBytes(String.valueOf(fileLong)));
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
      // delete local file!
      file.delete();
    }

    private void work() throws IOException, NoSuchAlgorithmException {
      String msg = readShortMessage();
      sockOut.write(Bytes.toBytes(LCIndexConstant.NO_MEANING_MSG));
      String targetName = readShortMessage(msg);
      if (LCIndexConstant.DELETE_HEAD_MSG.equals(msg)) {
        deleteFile(targetName);
      } else if (LCIndexConstant.REQUIRE_HEAD_MSG.equals(msg)) {
        // server check local, and say no if not exists
        if (!new File(targetName).exists()) {
          sockOut.write(LCIndexConstant.LCC_LOCAL_FILE_NOT_FOUND_MSG.getBytes());
          String hdfsPath = readShortMessage();
          CommitJobQueue.CommitJob commitJob =
              CommitJobQueue.getInstance().findJobByDestPath(new Path(hdfsPath));
          boolean processNow = false;
          if (commitJob != null) { // in commit
            sockOut.write(Bytes.toBytes(LCIndexConstant.FILE_IN_COMMIT_QUEUE_MSG));
            processNow = Boolean.valueOf(readShortMessage());
            CommitJobQueue.getInstance().promoteJob(commitJob, processNow);
          } else { // not in commit
            CompleteCompactionJobQueue.CompleteCompactionJob completeCompactJob =
                CompleteCompactionJobQueue.getInstance().findJobByDestPath(new Path(hdfsPath));
            if (completeCompactJob != null) { // in complete
              sockOut.write(Bytes.toBytes(LCIndexConstant.FILE_IN_COMPLETECOMPACTION_QUEUE_MSG));
              processNow = Boolean.valueOf(readShortMessage());
              CompleteCompactionJobQueue.getInstance().promoteJob(completeCompactJob, processNow);
            } else { // surely not on this server
              sockOut.write(LCIndexConstant.LCC_LOCAL_FILE_NOT_FOUND_MSG.getBytes());
              // System.out.println("winter LCCHFileMoverServer can not fild file: " + targetName);
              return;
            }
          }
          if (processNow) {
            transferFile(new File(targetName));
          }
        } else { // file exists, just return
          // File exists!
          File file = new File(targetName);
          if (!file.isFile()) {
            sockOut.write(LCIndexConstant.LCC_LOCAL_FILE_NOT_FOUND_MSG.getBytes());
            throw new IOException(
                "winter LCCHFileMoverServer found file but not a file (may be a dir?) : "
                    + targetName);
          }
          sockOut.write(LCIndexConstant.LCC_LOCAL_FILE_FOUND_MSG.getBytes());
          // last operation is write, read and discard
          readShortMessage();
          transferFile(file);
        }
      }
    }
  }
}
