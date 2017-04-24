package org.apache.hadoop.hbase.regionserver.index;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.regionserver.index.jobs.BasicJob;
import org.apache.hadoop.hbase.regionserver.index.jobs.BasicJob.JobStatus;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

public class JobExecutor implements Runnable {

  private static final Log LOG = LogFactory.getLog(JobExecutor.class);
  private static final long SLEEP_TIME = 1000;
  static int QUEUE_REPORT_INTERVAL = 2000; // for all queues!
  static int EMPTY_PRINT_INTERVAL = 100;
  public final String executorName;
  private final Object syncJobsObj;
  int queueCounter = 0; // all queues!
  int emptyQueueCounter = 0;
  int reachedMaxSize = 0;
  private List<BasicJob> jobs;
  public boolean keepRun = true;

  public JobExecutor(List<BasicJob> jobs, String executorName, Object obj) {
    this.jobs = jobs;
    this.executorName = executorName;
    this.syncJobsObj = obj;
  }

  @Override public void run() {
    while (keepRun) {
      try {
        QueueStatus ret = innerWork();
        if (ret == QueueStatus.EMPTY) {
          Thread.sleep(SLEEP_TIME);
        } else if (ret == QueueStatus.NO_ONE_READY) {
          Thread.sleep(SLEEP_TIME / 2);
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    LOG.info("LCIndex job executor: " + executorName + " exiting, " + jobs.size() + " jobs left");
    try {
      while (innerWork() != QueueStatus.EMPTY) {
        Thread.sleep(SLEEP_TIME / 2);
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    LOG.info("LCIndex job executor: " + executorName + " exiting");
  }

  private BasicJob findReadyJob() {
    synchronized (syncJobsObj) {
      Iterator<BasicJob> iter = jobs.iterator();
      while (iter.hasNext()) {
        BasicJob job = iter.next();
        if (job.getStatus() == JobStatus.FINISHED) { // jobs may be done due to promotion
          iter.remove();
        } else if (job.isReady()) {
          job.setStatus(JobStatus.DOING);
          return job;
        }
      }
    }
    return null;
  }

  // return true if empty
  private QueueStatus innerWork() throws InterruptedException, IOException {
    QueueStatus ret = QueueStatus.NO_ONE_READY;
    int size = 0;
    synchronized (syncJobsObj) {
      size = jobs.size();
    }
    if (reachedMaxSize < size) {
      LOG.info("LCIndex job class: " + executorName + " reaching new maxSize: " + size);
      reachedMaxSize = size;
    }
    if (size == 0) {
      if (++emptyQueueCounter == EMPTY_PRINT_INTERVAL) {
        emptyQueueCounter = 0;
        //         System.out.println("winter job class: " + jobName + " empty for: " + EMPTY_PRINT_INTERVAL
        //         + " times");
      }
      return QueueStatus.EMPTY;
    }
    BasicJob job = findReadyJob();
    if (job != null) { // one job is ready to run
      long waitTime = System.currentTimeMillis() - job.startTime;
      try {
        if (job.getStatus() != JobStatus.FINISHED) job.work();
      } catch (IOException e) {
        LOG.info("****winter IOException happens on job " + job.jobInfo());
        job.printRelatedJob();
        e.printStackTrace();
      } finally {
        long execTime = System.currentTimeMillis() - job.startTime - waitTime;
        job.setStatus(JobStatus.FINISHED);
        job.printDone(waitTime / 1000.0, execTime / 1000.0);
        synchronized (syncJobsObj) {
          jobs.remove(job);
        }
        ret = QueueStatus.DOING_ONE;
      }
    } else {
    }
    if (++queueCounter == QUEUE_REPORT_INTERVAL) {
      queueCounter = 0;
      // System.out.println("winter job class: " + jobName + " with queue size: " + size);
    }
    return ret;
  }

  enum QueueStatus {
    EMPTY, NO_ONE_READY, DOING_ONE;
  }
}
