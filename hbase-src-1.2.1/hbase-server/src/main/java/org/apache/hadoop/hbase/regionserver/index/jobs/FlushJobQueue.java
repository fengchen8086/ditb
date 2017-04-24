package org.apache.hadoop.hbase.regionserver.index.jobs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.index.userdefine.ColumnInfo;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.TimeRangeTracker;
import org.apache.hadoop.hbase.regionserver.index.lcindex.LCIndexWriter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

public class FlushJobQueue extends BasicJobQueue {
  private static final FlushJobQueue singleton = new FlushJobQueue();

  public static FlushJobQueue getInstance() {
    return singleton;
  }

  @Override public boolean checkJobClass(BasicJob job) {
    return job instanceof FlushJob;
  }

  @Override public String getJobQueueName() {
    return "FlushJob";
  }

  public FlushJob findJobByRawPath(Path rawPath) {
    FlushJob ret = null;
    synchronized (syncJobsObj) {
      for (BasicJob job : jobs) {
        FlushJob fJob = (FlushJob) job;
        if (rawPath.compareTo(fJob.rawHFilePath) == 0) {
          ret = fJob;
          break;
        }
      }
    }
    return ret;
  }

  public static class FlushJob extends BasicJob {
    private Path rawHFilePath; // tmp Path, used by CommitJob
    private HStore store;
    private LinkedList<KeyValue> queue;
    private TimeRangeTracker tracker;

    public FlushJob(HStore store, LinkedList<KeyValue> keyvalueQueue, Path pathName,
        TimeRangeTracker snapshotTimeRangeTracker) {
      super(FlushJobQueue.getInstance().getJobQueueName());
      this.store = store;
      this.queue = keyvalueQueue;
      this.rawHFilePath = pathName;
      this.tracker = snapshotTimeRangeTracker;
      printMessage("winter FlushJob construction, rawHFilePath: " + rawHFilePath);
    }

    @Override public boolean checkReady() { // flush is always true
      return true;
    }

    @Override public boolean correspondsTo(BasicJob j) throws IOException {
      throw new IOException("winter FlushJob.correspondsTo() should not called");
    }

    @Override public void printRelatedJob() {
      printMessage("winter flush job want to write " + rawHFilePath + ", just waiting");
    }

    @Override public void promoteRelatedJobs(boolean processNow) throws IOException {
      // flush has no related jobs, do nothing
    }

    @Override public void work() throws IOException {
      if (!queue.isEmpty()) {
        printMessage(
            "LCDBG, FlushJobQueue writer ranges [ " + Bytes.toInt(queue.getFirst().getRow())
                + ",,,, " + Bytes.toInt(queue.getLast().getRow()) + "]");
      }
      int queueSize = queue.size();
      LCIndexWriter writer2 = new LCIndexWriter(store, rawHFilePath, tracker);
//      printIndexSourceMap(store.indexTableRelation.getIndexColumnMap());
      writer2.processKeyValueQueue(queue);
      setJobDetail("flush, rawHFile=" + rawHFilePath + " with " + queueSize + " records");
    }

    private void printIndexSourceMap(Map<ColumnInfo, Set<ColumnInfo>> indexSourceMap) {
      if (indexSourceMap == null) {
        System.out.println("source is null");
        return;
      }
      System.out.println("source map.size=" + indexSourceMap.size());
      for (Map.Entry<ColumnInfo, Set<ColumnInfo>> entry : indexSourceMap.entrySet()) {
        System.out.println(
            "inside source map: " + entry.getKey() + " has " + entry.getValue().size() + " values");
      }
    }
  }
}
