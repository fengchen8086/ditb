package org.apache.hadoop.hbase.regionserver.index.jobs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.index.jobs.CompleteCompactionJobQueue.CompleteCompactionJob;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class ArchiveJobQueue extends BasicJobQueue {

  private static final ArchiveJobQueue singleton = new ArchiveJobQueue();

  public static ArchiveJobQueue getInstance() {
    return singleton;
  }

  @Override public boolean checkJobClass(BasicJob job) {
    return job instanceof ArchiveJob;
  }

  @Override public String getJobQueueName() {
    return "ArchiveJob";
  }

  /**
   * delete the old files used by related compaction job
   *
   * @author
   */
  public static class ArchiveJob extends BasicJob {

    private HStore store;
    private List<Path> compactedHFiles;
    private CompleteCompactionJob prevCompleteCompactJob;
    private boolean isMajor;
    private Path hdfsPathGenerated;

    public ArchiveJob(HStore store, Collection<StoreFile> compactedFiles, Path hdfsPath,
        CompleteCompactionJob completeCompactionJob, boolean isMajor) {
      super(ArchiveJobQueue.getInstance().getJobQueueName());
      this.store = store;
      compactedHFiles = new ArrayList<>(compactedFiles.size());
      for (StoreFile sf : compactedFiles)
        compactedHFiles.add(sf.getPath());
      this.prevCompleteCompactJob = completeCompactionJob;
      this.isMajor = isMajor;
      hdfsPathGenerated = hdfsPath;
      printMessage(
          "ArchiveJob construction, compaction generates hdfs HFile: " + hdfsPathGenerated);
    }

    @Override public boolean checkReady() {
      boolean ret = prevCompleteCompactJob == null
          || prevCompleteCompactJob.getStatus() == JobStatus.FINISHED;
      if (ret) {
        if (prevCompleteCompactJob == null) {
          printMessage("archive job ready because prevCompleteCompactJob == null");
        } else {
          printMessage("archive job ready because prevCompleteCompactJob has finished");
          prevCompleteCompactJob.printRelatedJob();
        }
      }
      return ret;
    }

    @Override public void printRelatedJob() {
      if (prevCompleteCompactJob == null) {
        printMessage("ArchiveJob.prevCompleteCompactJob is null, just waiting");
      } else {
        printMessage("ArchiveJob is waiting for prevCompleteCompactJob: " + prevCompleteCompactJob
            .jobInfo());
        prevCompleteCompactJob.printRelatedJob();
      }
    }

    @Override public boolean correspondsTo(BasicJob j) throws IOException {
      List<Path> paths = ((ArchiveJob) j).compactedHFiles;
      if (compactedHFiles == null && paths == null) return true;
      else if (compactedHFiles == null || paths == null) return false;
      else if (compactedHFiles.size() != paths.size()) {
        return false;
      } else {
        for (Path p1 : paths) {
          boolean found = false;
          for (Path p2 : compactedHFiles) {
            if (p1.equals(p2)) {
              found = true;
              break;
            }
          }
          if (!found) return false;
        }
      }
      return true;
    }

    @Override public void promoteRelatedJobs(boolean processNow) throws IOException {
      if (prevCompleteCompactJob != null) {
        prevCompleteCompactJob.promoteRelatedJobs(processNow);
      }
    }

    @Override public void work() throws IOException {
      for (Path path : compactedHFiles) {
        Path dir = store.getLCIndexParameters().getLocalDir(path);
        store.getLCIndexParameters().getLCIndexFileSystem().delete(dir, true);
        printMessage("archive in complete compact " + dir + ", hdfs: " + hdfsPathGenerated);
        if (store.getLCIndexParameters().getLCIndexFileSystem().listFiles(dir.getParent(), true).hasNext()
            == false) {
          store.getLCIndexParameters().getLCIndexFileSystem().delete(dir.getParent(), true);
          printMessage(
              "archive in complete delete " + dir.getParent() + ", hdfs: " + hdfsPathGenerated);
        }
      }
      setJobDetail("Archive hdfs HFile: " + hdfsPathGenerated);
    }
  }

}
