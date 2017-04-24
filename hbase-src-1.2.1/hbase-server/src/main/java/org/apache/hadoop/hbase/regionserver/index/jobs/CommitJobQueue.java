package org.apache.hadoop.hbase.regionserver.index.jobs;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.index.lcindex.LCIndexParameters;

import java.io.IOException;

public class CommitJobQueue extends BasicJobQueue {

  private static final CommitJobQueue singleton = new CommitJobQueue();

  public static CommitJobQueue getInstance() {
    return singleton;
  }

  @Override public boolean checkJobClass(BasicJob job) {
    return job instanceof CommitJob;
  }

  @Override public String getJobQueueName() {
    return "CommitJob";
  }

  public CommitJob findJobByDestPath(Path destPath) {
    synchronized (syncJobsObj) {
      for (BasicJob job : jobs) {
        CommitJob fJob = (CommitJob) job;
        if (destPath.compareTo(fJob.hdfsDestPath) == 0) {
          return fJob;
        }
      }
    }
    return null;
  }

  public static class CommitJob extends BasicJob {
    private FlushJobQueue.FlushJob relatedFlushJob = null;
    private HStore store;
    private Path hdfsTmpPath; // tmp Path
    private Path hdfsDestPath; // solid Path
    private LCIndexParameters indexParameters;

    public CommitJob(HStore store, Path src, Path dest) {
      super(CommitJobQueue.getInstance().getJobQueueName());
      this.store = store;
      this.hdfsTmpPath = src;
      this.hdfsDestPath = dest;
      relatedFlushJob = FlushJobQueue.getInstance().findJobByRawPath(hdfsTmpPath);
      indexParameters = store.getLCIndexParameters();
      printMessage("CommitJob construction, hdfsTmpPath: " + hdfsTmpPath + ", hdfsDestPath: "
          + hdfsDestPath);
    }

    @Override public boolean checkReady() {
      return relatedFlushJob == null || relatedFlushJob.getStatus() == JobStatus.FINISHED;
    }

    @Override public void printRelatedJob() {
      if (relatedFlushJob == null) {
        printMessage("CommitJob.relatedFlushJob is has been done, just waiting");
      } else {
        printMessage("CommitJob is waiting for relatedFlushJob: " + relatedFlushJob.jobInfo());
        relatedFlushJob.printRelatedJob();
      }
    }

    @Override public boolean correspondsTo(BasicJob j) throws IOException {
      // CommitJob only care about the hdfsDestPath
      return this.hdfsDestPath.equals(((CommitJob) j).hdfsDestPath);
    }

    @Override public void promoteRelatedJobs(boolean processNow) throws IOException {
      if (relatedFlushJob != null) {
        if (processNow) {
          if (relatedFlushJob.checkReady()) {
            relatedFlushJob.work(); // consider job and work, another method is needed
          }
        } else {
          FlushJobQueue.getInstance().putJobToQueueHead(relatedFlushJob);
        }
      }
    }

    @Override public void work() throws IOException {
      Path localTmpPath = indexParameters.getTmpDirPath(hdfsTmpPath);
      Path localDestPath = indexParameters.getLocalDir(hdfsDestPath);
      indexParameters.getLCIndexFileSystem().rename(localTmpPath, localDestPath);
      StringBuilder sb = new StringBuilder("sub files: ");
      for (FileStatus fs : store.getLCIndexParameters().getLCIndexFileSystem().listStatus(localDestPath)) {
        sb.append(fs.getPath()).append(",");
      }
      setJobDetail(
          "commit from " + localTmpPath + " to " + localDestPath + ", more " + sb.toString());
    }
  }
}
