package org.apache.hadoop.hbase.regionserver.index.jobs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.regionserver.index.JobExecutor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class BasicJobQueue {
  protected static final Log LOG = LogFactory.getLog(BasicJobQueue.class);
  protected static long SLEEP_TIME = 100;
  protected final Object syncJobsObj = new Object();
  protected List<BasicJob> jobs;
  protected JobExecutor executor = null;
  protected Thread execThread = null;
  boolean started = false;

  public void startThread() {
    if (!started) {
      started = true;
      jobs = new ArrayList<>();
      executor = new JobExecutor(jobs, getJobQueueName(), syncJobsObj);
      execThread = new Thread(executor);
      execThread.start();
      LOG.info("LCINFO, job queue " + getJobQueueName() + " starts");
    }
  }

  /**
   * if finished, return true
   *
   * @return
   */
  public boolean stopTheThread() {
    if (execThread == null) return true;
    if (executor.keepRun) executor.keepRun = false;
    if (execThread.isAlive()) {
      return false;
    }
    execThread = null;
    return true;
  }

  public void addJob(BasicJob job) {
    if (!executor.keepRun) {
      LOG.error("LCINFO, JobQueue thread is stopping, reject new job on " + executor);
    }
    synchronized (syncJobsObj) {
      jobs.add(job);
    }
  }

  // if not processNow, promote the current job to the head and promotes related jobs
  public void promoteJob(BasicJob rawJob, boolean processNow) throws IOException {
    if (!checkJobClass(rawJob)) {
      throw new IOException(
          "winter job queue " + getJobQueueName() + " meet job: " + rawJob.jobInfo());
    }
    BasicJob job = findJobInQueue(rawJob);
    if (job == null) {
      job = rawJob;
    }
    if (job.getStatus() == BasicJob.JobStatus.WAITING) {
      // 1. related job is waiting
      if (processNow) {
        if (job.checkReady()) { // process if ready
          job.work();
        } else { // promote related job and then work
          job.promoteRelatedJobs(processNow);
          if (!job.checkReady()) {
            throw new IOException(
                "LCDBG, job still not ready after upgrade related job: " + job.jobInfo());
          }
          job.work();
        }
      } else {
        putJobToQueueHead(job);
      }
    } else if (processNow) { // 2. related job is running and need to wait
      while (job.getStatus() != BasicJob.JobStatus.FINISHED) {
        try {
          Thread.sleep(SLEEP_TIME);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    } // 3. related job is running and not need to wait, just return
  }

  public void putJobToQueueHead(BasicJob job) throws IOException {
    synchronized (syncJobsObj) {
      if (jobs.contains(job)) {
        jobs.remove(job);
      }
      jobs.add(0, job);
    }
  }

  /**
   * @param job
   * @return the corresponding job, if waiting, remove the job from queue
   * @throws IOException
   */
  private BasicJob findJobInQueue(BasicJob job) throws IOException {
    synchronized (syncJobsObj) {
      for (BasicJob j : jobs) {
        if (job.correspondsTo(j)) {
          if (j.getStatus() == BasicJob.JobStatus.WAITING) {
            jobs.remove(j);
          }
          return j;
        }
      }
    }
    return null;
  }

  public abstract boolean checkJobClass(BasicJob job);

  public abstract String getJobQueueName();

  public double getSleepFactor() {
    return 0;
    // int length = jobs.size();
    // if (length > 10) length = 10;
    // return 0.01 * length;
  }

  public int getJobListSize() {
    synchronized (syncJobsObj) {
      return jobs.size();
    }
  }
}
