package org.apache.hadoop.hbase.regionserver.index.jobs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public abstract class BasicJob {
  protected static final Log LOG = LogFactory.getLog(BasicJob.class);
  protected static AtomicLong idCounter = new AtomicLong(0);
  protected static boolean printForDebug = true;
  private static long READY_PRINT_INTERVAL = 10 * 1000;
  public long id;
  public long startTime;
  protected JobStatus status;
  private String jobName;
  private String jobDetail;
  private long lastPrintTime;

  public abstract boolean checkReady();

  public BasicJob(String jobName) {
    this.jobName = jobName;
    id = idCounter.getAndIncrement();
    startTime = System.currentTimeMillis();
    lastPrintTime = startTime;
    status = JobStatus.WAITING;
    jobDetail = null;
  }

  public boolean isReady() {
    long now = System.currentTimeMillis();
    if (checkReady()) {
      printMessage("LCIndex job ready after " + (now - startTime) / 1000.0 + " seconds");
      return true;
    } else if ((now - lastPrintTime) > READY_PRINT_INTERVAL) {
      lastPrintTime = now;
      printMessage("LCIndex job still not ready after " + (now - startTime) / 1000.0 + " seconds");
      printRelatedJob();
    }
    return false;
  }

  public abstract void printRelatedJob();

  public abstract boolean correspondsTo(BasicJob j) throws IOException;

  public abstract void promoteRelatedJobs(boolean processNow) throws IOException;

  public abstract void work() throws IOException;

  public JobStatus getStatus() {
    return status;
  }

  public void setStatus(JobStatus status) {
    this.status = status;
  }

  protected void printMessage(String msg) {
    if (printForDebug) LOG.info(msg + ", this job=" + jobInfo());
  }

  public String jobInfo() {
    return "[" + id + ":" + jobName + ":" + status + "]";
  }

  public void printDone(double waitTime, double execTime) {
    printMessage("job " + jobInfo() + " waits " + waitTime + " seconds and runs " + execTime
        + " seconds for " + jobDetail);
  }

  protected void setJobDetail(String jobDetail) {
    this.jobDetail = jobDetail;
  }

  public enum JobStatus {
    WAITING, DOING, FINISHED
  }
}
