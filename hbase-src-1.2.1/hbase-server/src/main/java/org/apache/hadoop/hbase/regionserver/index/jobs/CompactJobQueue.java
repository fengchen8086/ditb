package org.apache.hadoop.hbase.regionserver.index.jobs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFile.Reader;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionThroughputController;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionThroughputControllerFactory;
import org.apache.hadoop.hbase.regionserver.compactions.DefaultCompactor;
import org.apache.hadoop.hbase.regionserver.index.jobs.CommitJobQueue.CommitJob;
import org.apache.hadoop.hbase.regionserver.index.jobs.RemoteJobQueue.RemoteJob;
import org.apache.hadoop.hbase.regionserver.index.lcindex.LCIndexParameters;
import org.apache.hadoop.hbase.regionserver.index.lcindex.LCIndexWriter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.TreeMap;
import java.util.TreeSet;

public class CompactJobQueue extends BasicJobQueue {

  private static final CompactJobQueue singleton = new CompactJobQueue();

  public static CompactJobQueue getInstance() {
    return singleton;
  }

  @Override public boolean checkJobClass(BasicJob job) {
    return job instanceof CompactJob;
  }

  @Override public String getJobQueueName() {
    return "CompactJob";
  }

  public CompactJob findJobByDestPath(Path destPath) {
    synchronized (syncJobsObj) {
      for (BasicJob job : jobs) {
        CompactJob fJob = (CompactJob) job;
        if (destPath.compareTo(fJob.tmpHDFSPath) == 0) {
          return fJob;
        }
      }
    }
    return null;
  }

  public static class RebuildCompactJob extends CompactJob {

    private Path majorMovedPath = null; // solid path
    private Reader reader = null;

    public RebuildCompactJob(HStore store, CompactionRequest request, Path writtenPath)
        throws IOException {
      super(store, request, writtenPath);
      StringBuilder sb = new StringBuilder();
      sb.append("RebuildCompactJob construction, hdfsPath: ").append(tmpHDFSPath);
      sb.append(", with ").append(request.getFiles().size())
          .append(" store files compacted, they are: ");
      for (StoreFile sf : request.getFiles()) {
        sb.append(sf.getPath()).append(", ");
      }
      printMessage(sb.toString());
    }

    public void setMajorReady(Path destPath, Reader reader) {
      this.majorMovedPath = destPath;
      this.reader = reader;
      printMessage("set major compaction ready in path: " + destPath + ", reader.length()=" + reader
          .length());
    }

    @Override public boolean checkReady() {
      return majorMovedPath != null && reader != null;
    }

    @Override public void printRelatedJob() {
      System.out.println(
          "major compact job has no related job, want " + tmpHDFSPath + ", now majorMovedPath is: "
              + majorMovedPath + ", reader is null: " + (reader == null));
    }

    @Override public void work() throws IOException {
      Queue<KeyValue> queue = parseKeyValueFromReader();
      LCIndexWriter writer2 = new LCIndexWriter(store, tmpHDFSPath, null);
      writer2.processKeyValueQueue(queue);
      setJobDetail("rebuild " + queue.size() + " records, tmpHDFSPath=" + tmpHDFSPath);
    }

    private Queue<KeyValue> parseKeyValueFromReader() throws IOException {
      HFile.Reader reader = HFile
          .createReader(store.getFileSystem(), majorMovedPath, store.getCacheConfig(), store.conf);
      HFileScanner scanner = reader.getScanner(false, false, false);
      scanner.seekTo();
      Queue<KeyValue> queue = new LinkedList<>();
      do {
        KeyValue cell = (KeyValue) scanner.getKeyValue();
        queue.offer(cell);
      } while (scanner.next());
      reader.close();
      return queue;
    }

    private void winterTestingStoreFile(StoreFile sf) throws IOException {
      StoreFileScanner compactedFileScanner = sf.getReader().getStoreFileScanner(false, false);
      KeyValue startKey =
          KeyValueUtil.createFirstOnRow(HConstants.EMPTY_START_ROW, HConstants.LATEST_TIMESTAMP);
      compactedFileScanner.seek(startKey);
      KeyValue kv;
      int n = 0;
      while ((kv = (KeyValue) compactedFileScanner.next()) != null) {
        LOG.info("LCDBG, show kv: " + Bytes.toInt(kv.getRow()));
        ++n;
      }
      LOG.info("LCDBG, reader has: " + n + " in " + sf.getPath());
      compactedFileScanner.close();
    }

    private void winterTestingHFile(Path file) throws IOException {
      HFile.Reader reader =
          HFile.createReader(store.getFileSystem(), file, store.getCacheConfig(), store.conf);
      HFileScanner scanner = reader.getScanner(false, false, false);
      scanner.seekTo();
      int n = 0;
      do {
        Cell cell = scanner.getKeyValue();
        ++n;
      } while (scanner.next());
      LOG.info("LCDBG, HFile has: " + n + " in " + file);
    }

    @Override public void promoteRelatedJobs(boolean processNow) throws IOException {
      System.out.println("RebuildCompact job have no job to promote");
    }
  }

  public static class NormalCompactJob extends CompactJob {
    private List<CommitJob> relatedJobs;
    private TreeMap<byte[], List<Path>> iFileMap;
    private TreeMap<Path, BasicJob> missingPathMap;
    protected LCIndexParameters indexParameters;

    public NormalCompactJob(HStore store, CompactionRequest request, Path writtenPath)
        throws IOException {
      super(store, request, writtenPath);
      printMessage("NormalCompactJob construction, hdfsPath: " + tmpHDFSPath);
      relatedJobs = new ArrayList<>();
      iFileMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      missingPathMap = new TreeMap<>();
      indexParameters = store.getLCIndexParameters();
      // LC_Home/[tableName]/[regionId]/.tmp/[HFileId].lcindex
      for (Map.Entry<byte[], TreeSet<byte[]>> entry : store.indexTableRelation.getIndexFamilyMap()
          .entrySet()) {
        for (byte[] qualifier : entry.getValue()) {
          List<Path> iFilesToBeCompacted = new ArrayList<>();
          for (StoreFile compactedHFile : request.getFiles()) {
            // if family not match, skip
            if (!compactedHFile.getPath().getParent().getName()
                .equals(Bytes.toString(entry.getKey()))) {
              continue;
            }
            Path iFile = indexParameters.getLocalIFile(compactedHFile.getPath(), qualifier);
            iFilesToBeCompacted.add(iFile);
            LOG.info(
                "LCDBG, minor compact need iFile " + iFile.toString() + " for qualifier " + Bytes
                    .toString(qualifier) + ", target: " + tmpHDFSPath);
            if (!indexParameters.getLCIndexFileSystem().exists(iFile)) {
              printMessage("finding the iFile to be compacted: " + iFile);
              // when missing, first find it in commit job, then in remote job
              BasicJob job =
                  CommitJobQueue.getInstance().findJobByDestPath(compactedHFile.getPath());
              if (job == null) {
                job = CompleteCompactionJobQueue.getInstance()
                    .findJobByDestPath(compactedHFile.getPath());
                if (job == null) {
                  job = new RemoteJob(store, iFile, compactedHFile.getPath(), false);
                  RemoteJobQueue.getInstance().addJob(job);
                }
              }
              printMessage("LCDBG, missing file " + iFile + " on HFile " + compactedHFile.getPath()
                  + " relies on job: " + job.getClass().getName());
              missingPathMap.put(iFile, job);
            }
          }
          iFileMap.put(qualifier, iFilesToBeCompacted);
          printMessage(
              "LCDBG, minor compact on qualifier " + Bytes.toString(qualifier) + " with iFile.size="
                  + iFilesToBeCompacted.size() + ", target: " + tmpHDFSPath);
        }
      }
    }

    @Override public boolean checkReady() {
      // if all related jobs are finished, then report ready
      if (missingPathMap.size() == 0) return true;
      List<Path> toDelete = new ArrayList<>();
      for (Entry<Path, BasicJob> entry : missingPathMap.entrySet()) {
        if (entry.getValue().getStatus() == JobStatus.FINISHED) {
          toDelete.add(entry.getKey());
          printMessage("previous job ready, file ready: " + entry.getKey());
        }
      }
      for (Path p : toDelete) {
        missingPathMap.remove(p);
      }
      return missingPathMap.size() == 0;
    }

    @Override public void promoteRelatedJobs(boolean processNow) throws IOException {
      List<BasicJob> promotedJobs = new ArrayList<BasicJob>();
      for (Entry<Path, BasicJob> entry : missingPathMap.entrySet()) {
        if (promotedJobs.contains(entry.getValue())) {
          continue;
        } else {
          promotedJobs.add(entry.getValue());
          // commit or remote
          entry.getValue().promoteRelatedJobs(processNow);
        }
      }
    }

    @Override public void printRelatedJob() {
      printMessage("minor compact hdfsPath is: " + tmpHDFSPath + ", list waiting jobs: ");
      for (Entry<Path, BasicJob> entry : missingPathMap.entrySet()) {
        printMessage(
            "minor compact job for path: " + entry.getKey() + " waiting for another job: " + entry
                .getValue().jobInfo());
        entry.getValue().printRelatedJob();
      }
    }

    @Override public void work() throws IOException {
      DefaultCompactor compactor = (DefaultCompactor) store.getStoreEngine().getCompactor();
      CompactionThroughputController controller = CompactionThroughputControllerFactory
          .create(store.getHRegion().getRegionServerServices(), store.conf);
      for (Entry<byte[], List<Path>> entry : iFileMap.entrySet()) {
        LOG.info(
            "compacting files for qualifier: " + Bytes.toString(entry.getKey()) + ", sub file.size="
                + entry.getValue().size() + ", target: " + tmpHDFSPath);
        List<StoreFile> lcStoreFiles = new ArrayList<>();
        List<Path> statFiles = new ArrayList<>();
        for (Path iFile : entry.getValue()) {
          StoreFile sf = new StoreFile(store.getLCIndexParameters().getLCIndexFileSystem(), iFile, store.conf,
              store.cacheConf, store.getFamily().getBloomFilterType());
          sf.createReader();
          lcStoreFiles.add(sf);
          statFiles.add(LCIndexParameters.iFileToSFile(iFile));
          printMessage("minor compact need iFile in compaction on qualifier " + Bytes
              .toString(entry.getKey()) + ": " + iFile + ", target: " + tmpHDFSPath);
        }
        CompactionRequest lcRequest = new CompactionRequest(lcStoreFiles);
        lcRequest.setIsMajor(true, true);
        Path destPath = indexParameters.getTmpIFilePath(tmpHDFSPath, entry.getKey());
        // compact stat file, then compact storefile
        compactStatFiles(store.getLCIndexParameters().getLCIndexFileSystem(), statFiles,
            LCIndexParameters.iFileToSFile(destPath));
        // compact IFiles directly
        List<Path> ret =
            compactor.compactLCIndexIFiles(store, lcRequest, controller, null, destPath, true);
        for (StoreFile sf : lcStoreFiles) {
          sf.closeReader(true);
        }
        for (Path p : ret) {
          printMessage("minor compact flush to: " + p + " in compaction on qualifier " + Bytes
              .toString(entry.getKey()) + ", target: " + tmpHDFSPath);
        }
        printMessage(
            "minor compact flush done in compaction on qualifier " + Bytes.toString(entry.getKey())
                + ", target: " + tmpHDFSPath);
      }
      setJobDetail("normal compact on hdfsPath: " + tmpHDFSPath);
    }

    public void compactStatFiles(FileSystem fs, List<Path> statPaths, Path targetPath)
        throws IOException {
      if (statPaths.size() == 0) return;
      BufferedReader[] readers = new BufferedReader[statPaths.size()];
      for (int i = 0; i < readers.length; ++i) {
        readers[i] = new BufferedReader(new InputStreamReader(fs.open(statPaths.get(i))));
      }
      BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(targetPath, true)));
      while (true) {
        String line = readers[0].readLine();
        if (line == null) break;
        long temp = Long.parseLong(line);
        for (int i = 1; i < readers.length; ++i) {
          line = readers[i].readLine();
          temp += Long.parseLong(line);
        }
        bw.write(temp + "\n");
      }
      for (BufferedReader br : readers) {
        br.close();
      }
      bw.close();
    }

  }

  public static abstract class CompactJob extends BasicJob {
    protected HStore store;
    protected CompactionRequest request;
    protected Path tmpHDFSPath; // tmp HFile new compacted

    public CompactJob(HStore store, final CompactionRequest request, final Path writtenPath)
        throws IOException {
      super(CompactJobQueue.getInstance().getJobQueueName());
      this.store = store;
      this.request = request;
      this.tmpHDFSPath = writtenPath;
    }

    @Override public boolean correspondsTo(BasicJob j) throws IOException {
      return this.tmpHDFSPath.equals(((CompactJob) j).tmpHDFSPath);
    }
  }
}
