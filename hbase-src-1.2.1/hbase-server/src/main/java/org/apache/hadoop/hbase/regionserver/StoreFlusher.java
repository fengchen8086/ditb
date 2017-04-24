/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */

package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.index.IndexType;
import org.apache.hadoop.hbase.index.client.IndexDescriptor;
import org.apache.hadoop.hbase.index.mdhbase.MDIndex;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.regionserver.compactions.Compactor;
import org.apache.hadoop.hbase.regionserver.index.IndexKeyValue;
import org.apache.hadoop.hbase.regionserver.index.IndexWriter;
import org.apache.hadoop.hbase.regionserver.index.jobs.FlushJobQueue;
import org.apache.hadoop.hbase.regionserver.index.lmdindex.LMDIndexWriter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.mortbay.log.Log;

import java.io.IOException;
import java.util.*;

/**
 * Store flusher interface. Turns a snapshot of memstore into a set of store files (usually one).
 * Custom implementation can be provided.
 */
@InterfaceAudience.Private abstract class StoreFlusher {
  protected Configuration conf;
  protected Store store;

  public StoreFlusher(Configuration conf, Store store) {
    this.conf = conf;
    this.store = store;
  }

  /**
   * Turns a snapshot of memstore into a set of store files.
   *
   * @param snapshot         Memstore snapshot.
   * @param cacheFlushSeqNum Log cache flush sequence number.
   * @param status           Task that represents the flush operation and may be updated with status.
   * @return List of files written. Can be empty; must not be null.
   */
  public abstract List<Path> flushSnapshot(MemStoreSnapshot snapshot, long cacheFlushSeqNum,
      MonitoredTask status) throws IOException;

  public abstract List<Path> flushSnapshot(MemStoreSnapshot snapshot, long cacheFlushSeqNum,
      MonitoredTask status, HStore store) throws IOException;

  protected void finalizeWriter(StoreFile.Writer writer, long cacheFlushSeqNum,
      MonitoredTask status) throws IOException {
    // Write out the log sequence number that corresponds to this output
    // hfile. Also write current time in metadata as minFlushTime.
    // The hfile is current up to and including cacheFlushSeqNum.
    status.setStatus("Flushing " + store + ": appending metadata");
    writer.appendMetadata(cacheFlushSeqNum, false);
    status.setStatus("Flushing " + store + ": closing flushed file");
    writer.close();
  }

  /**
   * Creates the scanner for flushing snapshot. Also calls coprocessors.
   *
   * @param snapshotScanner
   * @param smallestReadPoint
   * @return The scanner; null if coprocessor is canceling the flush.
   */
  protected InternalScanner createScanner(KeyValueScanner snapshotScanner, long smallestReadPoint)
      throws IOException {
    InternalScanner scanner = null;
    if (store.getCoprocessorHost() != null) {
      scanner = store.getCoprocessorHost().preFlushScannerOpen(store, snapshotScanner);
    }
    if (scanner == null) {
      Scan scan = new Scan();
      scan.setMaxVersions(store.getScanInfo().getMaxVersions());
      scanner = new StoreScanner(store, store.getScanInfo(), scan,
          Collections.singletonList(snapshotScanner), ScanType.COMPACT_RETAIN_DELETES,
          smallestReadPoint, HConstants.OLDEST_TIMESTAMP);
    }
    assert scanner != null;
    if (store.getCoprocessorHost() != null) {
      try {
        return store.getCoprocessorHost().preFlush(store, scanner);
      } catch (IOException ioe) {
        scanner.close();
        throw ioe;
      }
    }
    return scanner;
  }

  /**
   * Performs memstore flush, writing data from scanner into sink.
   *
   * @param scanner           Scanner to get data from.
   * @param sink              Sink to write data to. Could be StoreFile.Writer.
   * @param smallestReadPoint Smallest read point used for the flush.
   */
  protected void performFlush(InternalScanner scanner, Compactor.CellSink sink,
      long smallestReadPoint) throws IOException {
    int compactionKVMax =
        conf.getInt(HConstants.COMPACTION_KV_MAX, HConstants.COMPACTION_KV_MAX_DEFAULT);

    ScannerContext scannerContext =
        ScannerContext.newBuilder().setBatchLimit(compactionKVMax).build();

    List<Cell> kvs = new ArrayList<Cell>();
    boolean hasMore;
    do {
      hasMore = scanner.next(kvs, scannerContext);
      if (!kvs.isEmpty()) {
        for (Cell c : kvs) {
          // If we know that this KV is going to be included always, then let us
          // set its memstoreTS to 0. This will help us save space when writing
          // to
          // disk.
          sink.append(c);
        }
        kvs.clear();
      }
    } while (hasMore);
  }

  protected void performFlushWithIndex(InternalScanner scanner, StoreFile.Writer writer,
      long smallestReadPoint, HStore store) throws IOException {
    int compactionKVMax =
        conf.getInt(HConstants.COMPACTION_KV_MAX, HConstants.COMPACTION_KV_MAX_DEFAULT);

    ScannerContext scannerContext =
        ScannerContext.newBuilder().setBatchLimit(compactionKVMax).build();

    List<Cell> kvs = new ArrayList<Cell>();
    boolean hasMore;

    IndexWriter indexWriter = null;
    TreeSet<IndexKeyValue> irIndexSet = null;
    if (store.getIndexType() == IndexType.IRIndex) {
      irIndexSet = new TreeSet<IndexKeyValue>(store.irIndexComparator);
      for (Map.Entry<byte[], IndexDescriptor> entry : store.irIndexMap.entrySet()) {
        Log.info("LCINFO: flush with irindex qualifier: " + Bytes.toString(entry.getKey())
            + ", index table is: " + entry.getValue());
      }
    }

    LinkedList<KeyValue> keyvalueQueue = null;
    if (store.getIndexType() == IndexType.LCIndex || store.getIndexType() == IndexType.LMDIndex_S
        || store.getIndexType() == IndexType.LMDIndex_D) {
      // save keyvalue because snapshot will be cleaned soon
      keyvalueQueue = new LinkedList<>();
    }
    try {
      do {
        hasMore = scanner.next(kvs, scannerContext);
        if (!kvs.isEmpty()) {
          for (Cell c : kvs) {
            writer.append(c);
            // LCTODO: how to handle delete and remove?
            if (store.getIndexType() == IndexType.IRIndex) {
              if (store.irIndexMap.containsKey(CellUtil.cloneQualifier(c))) {
                irIndexSet.add(new IndexKeyValue(c));
              }
            } else if (store.getIndexType() == IndexType.LCIndex) {
              keyvalueQueue.add((KeyValue) c);
            } else if (store.getIndexType() == IndexType.LMDIndex_S
                || store.getIndexType() == IndexType.LMDIndex_D) {
              keyvalueQueue.add((KeyValue) c);
            }
          }
          kvs.clear();
        }
      } while (hasMore);
      if (store.getIndexType() == IndexType.IRIndex && !irIndexSet.isEmpty()) {
        // LCDBG: committing store fromsnapshotTimeRangeTracker
        // hdfs://localhost:9000/hbase/data/hbase/meta/1588230740/.tmp/c1aaec13c7ab48669e8aeeab66bdf1e1
        // to
        // hdfs://localhost:9000/hbase/data/hbase/meta/1588230740/info/c1aaec13c7ab48669e8aeeab66bdf1e1
        indexWriter = store.createIndexWriterInTmp(writer.getPath());
        for (IndexKeyValue tmpkv : irIndexSet) {
          indexWriter.append(tmpkv);
        }
      } else if (store.getIndexType() == IndexType.LCIndex) {
        FlushJobQueue.FlushJob flushJob =
            new FlushJobQueue.FlushJob(store, keyvalueQueue, writer.getPath(),
                writer.timeRangeTracker);
        FlushJobQueue.getInstance().addJob(flushJob);
      } else if (store.getIndexType() == IndexType.LMDIndex_S
          || store.getIndexType() == IndexType.LMDIndex_D) {
        LMDIndexWriter lmdIndexWriter =
            new LMDIndexWriter(store, writer.getPath(), writer.timeRangeTracker, "FLUSH");
        lmdIndexWriter.processKeyValueQueue(keyvalueQueue);
      }
    } catch (IOException e) {
      throw e;
    } finally {
      if (indexWriter != null) indexWriter.close();
    }
  }
}
