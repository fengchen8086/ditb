package org.apache.hadoop.hbase.regionserver.index.jobs;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.index.jobs.CompactJobQueue.CompactJob;
import org.apache.hadoop.hbase.regionserver.index.jobs.CompactJobQueue.RebuildCompactJob;

import java.io.IOException;

public class CompleteCompactionJobQueue extends BasicJobQueue {

  private static final CompleteCompactionJobQueue singleton = new CompleteCompactionJobQueue();

  public static CompleteCompactionJobQueue getInstance() {
    return singleton;
  }

  @Override public boolean checkJobClass(BasicJob job) {
    return job instanceof CompleteCompactionJob;
  }

  @Override public String getJobQueueName() {
    return "CompleteCompactJob";
  }

  public CompleteCompactionJob findJobByDestPath(Path destPath) {
    synchronized (syncJobsObj) {
      for (BasicJob job : jobs) {
        CompleteCompactionJob fJob = (CompleteCompactionJob) job;
        LOG.info("cur: " + fJob.hdfsPath.toString() + ", target: " + destPath.toString());
        if (destPath.compareTo(fJob.hdfsPath) == 0) {
          return fJob;
        }
      }
    }
    return null;
  }

  /**
   * move the new written file from tmp path to online path && relies on minor compaction job!
   *
   * @author
   */
  public static class CompleteCompactionJob extends BasicJob {
    public Path tmpHDFSPath;
    public Path hdfsPath; // solid Path, used for search
    private HStore store;
    private CompactJob prevCompactJob;

    public CompleteCompactionJob(HStore store, Path tmpHDFSPath, Path hdfsPath,
        CompactJob minorCompactJob) {
      super(CompleteCompactionJobQueue.getInstance().getJobQueueName());
      this.store = store;
      this.tmpHDFSPath = tmpHDFSPath;
      this.hdfsPath = hdfsPath;
      this.prevCompactJob = minorCompactJob;
      String str = prevCompactJob == null ?
          "null" :
          String.valueOf(prevCompactJob instanceof RebuildCompactJob);
      printMessage(
          "CompleteCompactionJob construction, tmpHDFSPath: " + tmpHDFSPath + ", hdfs path: "
              + hdfsPath + ", prevCompactJob is RebuildCompactJob: " + str);
    }

    @Override public boolean checkReady() {
      boolean ret = prevCompactJob == null || prevCompactJob.getStatus() == JobStatus.FINISHED;
      if (ret) {
        if (prevCompactJob == null) {
          printMessage("CompleteCompactionJob ready because prevCompact job is null");
        } else {
          printMessage(
              "CompleteCompactionJob ready because prevCompact job is finished, relatedFlushJob id: "
                  + prevCompactJob.id);
          prevCompactJob.printRelatedJob();
        }
      }
      return ret;
    }

    @Override public void printRelatedJob() {
      if (prevCompactJob == null) {
        printMessage("CompleteCompactionJob.prevCompactJob is null, just waiting");
      } else {
        printMessage(
            "CompleteCompactionJob is waiting for prevCompactJob: " + prevCompactJob.jobInfo());
        prevCompactJob.printRelatedJob();
      }
    }

    @Override public boolean correspondsTo(BasicJob j) throws IOException {
      // CompleteCompactionJob only care about the compactedFile
      return this.tmpHDFSPath.equals(((CompleteCompactionJob) j).tmpHDFSPath);
    }

    @Override public void promoteRelatedJobs(boolean processNow) throws IOException {
      if (prevCompactJob != null) {
        prevCompactJob.promoteRelatedJobs(processNow);
      }
    }

    @Override public void work() throws IOException {
      Path localTmpPath = store.getLCIndexParameters().getTmpDirPath(tmpHDFSPath);
      Path localDestPath = store.getLCIndexParameters().getLocalDir(hdfsPath);
      store.getLCIndexParameters().getLCIndexFileSystem().rename(localTmpPath, localDestPath);
      StringBuilder sb = new StringBuilder("sub files: ");
      for (FileStatus fs : store.getLCIndexParameters().getLCIndexFileSystem().listStatus(localDestPath)) {
        sb.append(fs.getPath()).append(",");
      }
      setJobDetail(
          "complete job move file done, from " + localTmpPath + " to " + localDestPath + ", more "
              + sb.toString());
    }
  }

}
