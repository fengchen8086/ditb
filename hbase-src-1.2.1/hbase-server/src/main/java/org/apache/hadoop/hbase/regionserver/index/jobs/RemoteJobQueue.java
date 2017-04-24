package org.apache.hadoop.hbase.regionserver.index.jobs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.index.lcindex.LCCHFileMoverClient;
import org.apache.hadoop.hbase.regionserver.index.lcindex.LCCHFileMoverClient.RemoteStatus;

import java.io.IOException;

public class RemoteJobQueue extends BasicJobQueue {

  private static final RemoteJobQueue singleton = new RemoteJobQueue();

  public static RemoteJobQueue getInstance() {
    return singleton;
  }

  @Override public boolean checkJobClass(BasicJob job) {
    return job instanceof RemoteJob;
  }

  @Override public String getJobQueueName() {
    return "RemoteJob";
  }

  public static class RemoteJob extends BasicJob {
    private HStore store;
    private Path realPath;
    private Path hdfsPath;
    private boolean isDelete;
    private RemoteStatus lastTime;

    /**
     * @param store
     * @param path
     * @param isDelete true for delete, false for get
     */
    public RemoteJob(HStore store, Path path, Path hdfsPath, boolean isDelete) {
      super(RemoteJobQueue.getInstance().getJobQueueName());
      this.store = store;
      this.realPath = path;
      this.hdfsPath = hdfsPath;
      this.isDelete = isDelete;
      this.lastTime = RemoteStatus.UNKNOWN;
      printMessage(
          "winter RemoteJob construction, read file: " + realPath + ", hdfsPath: " + hdfsPath
              + ", isDelete: " + isDelete);
      new IOException(
          "winter RemoteJob construction, read file: " + realPath + ", hdfsPath: " + hdfsPath
              + ", isDelete: " + isDelete).printStackTrace();
    }

    @Override public boolean checkReady() {
      // try to get the file remotely! if all remote jobs are finished, then return yes
      return true;
    }

    @Override public void printRelatedJob() {
      printMessage(
          "winter RemoteJob want file: " + realPath + ", hdfsPath: " + hdfsPath + ", isDelete: "
              + isDelete);
    }

    @Override public boolean correspondsTo(BasicJob j) throws IOException {
      return this.hdfsPath.equals(((RemoteJob) j).hdfsPath);
    }

    @Override public void promoteRelatedJobs(boolean processNow) throws IOException {
      // this need to operator remotely
      if (lastTime != RemoteStatus.SUCCESS) {
        lastTime = operateRemoteLCLocalFile(store, realPath, hdfsPath, processNow, isDelete);
        if (lastTime != RemoteStatus.SUCCESS && lastTime != RemoteStatus.IN_QUEUE_WAITING) {
          throw new IOException(
              "winter remote job fail for file: " + realPath + ", hdfsPath: " + hdfsPath
                  + ", RemoteStatus: " + lastTime);
        }
      }
    }

    @Override public void work() throws IOException {
      if (lastTime != RemoteStatus.SUCCESS) {
        RemoteStatus ret = operateRemoteLCLocalFile(store, realPath, hdfsPath, true, isDelete);
        if (ret != RemoteStatus.SUCCESS) {
          throw new IOException(
              "winter remote job fail for file: " + realPath + ", hdfsPath: " + hdfsPath
                  + ", RemoteStatus: " + ret + ", isDelete: " + isDelete);
        }
        setJobDetail("RemoteJob read file: " + realPath + ", hdfsPath: " + hdfsPath + ", isDelete: "
            + isDelete);
      }
    }

    public RemoteStatus operateRemoteLCLocalFile(HStore store, Path realPath, Path hdfsPath,
        boolean processNow, boolean isDelete) throws IOException {
      LCCHFileMoverClient moverClient;
      if (store.lcPrevHoldServer != null) {
        moverClient = new LCCHFileMoverClient(store.lcPrevHoldServer, store.conf);
        RemoteStatus ret = isDelete ?
            moverClient.deleteRemoteFile(parsePath(realPath)) :
            moverClient.copyRemoteFile(parsePath(realPath), hdfsPath, processNow);
        if (ret == RemoteStatus.SUCCESS || ret == RemoteStatus.IN_QUEUE_WAITING) {
          printMessage(
              "winter succeed in " + (isDelete ? "deleting " : "finding ") + realPath + " on "
                  + store.lcPrevHoldServer + ", status: " + ret);
          return ret;
        }
      }
      for (String hostname : store.getLCIndexParameters().lcRegionServerHostnames) {
        if (hostname.equals(store.lcPrevHoldServer)) {
          continue;
        }
        moverClient = new LCCHFileMoverClient(hostname, store.conf);
        RemoteStatus ret = isDelete ?
            moverClient.deleteRemoteFile(parsePath(realPath)) :
            moverClient.copyRemoteFile(parsePath(realPath), hdfsPath, processNow);
        if (ret == RemoteStatus.SUCCESS || ret == RemoteStatus.IN_QUEUE_WAITING) {
          printMessage(
              "winter succeed in " + (isDelete ? "deleting " : "finding ") + realPath + " on "
                  + hostname + ", status: " + ret);
          return ret;
        }
      }
      if (processNow) {
        printMessage("winter failed in " + (isDelete ? "deleting " : "finding ") + realPath
            + " on all hosts");
      }
      store.lcPrevHoldServer = null;
      return RemoteStatus.NOT_EXIST;
    }

    public static String parsePath(Path p) {
      // p = file://xxxx/xxx/xxxx, trans to /xxxx/xxx/xxxx
      int depth = p.depth();
      String str = "";
      while (depth > 0) {
        str = Path.SEPARATOR + p.getName() + str;
        p = p.getParent();
        --depth;
      }
      return str;
    }

  }

}
