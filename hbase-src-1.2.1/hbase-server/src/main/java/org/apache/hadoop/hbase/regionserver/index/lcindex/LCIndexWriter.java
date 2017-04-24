package org.apache.hadoop.hbase.regionserver.index.lcindex;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.index.userdefine.IndexPutParser;
import org.apache.hadoop.hbase.index.userdefine.IndexTableRelation;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.TimeRangeTracker;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.TreeSet;

public class LCIndexWriter {
  static final Log LOG = LogFactory.getLog(LCIndexWriter.class);
  // LC_Home/[tableName]/[regionId]/.tmp/[HFileId].lcindex
  private final HStore store;
  private final IndexTableRelation tableRelation;
  private final Path hdfsTmpPath;
  private final TimeRangeTracker tracker;
  private final LCIndexParameters indexParameters;
  // save stat
  //  private final Map<byte[], LCStatInfo2> statMap;
  private final Map<TableName, LCStatInfo2> statMap;

  public LCIndexWriter(HStore store, Path hdfsTmpPath, TimeRangeTracker tracker)
      throws IOException {
    this.store = store;
    this.hdfsTmpPath = hdfsTmpPath;
    this.tableRelation = store.indexTableRelation;
    indexParameters = store.getLCIndexParameters();
    this.statMap = LCStatInfo2.parseStatString(tableRelation,
        store.getHRegion().getTableDesc().getValue(LCIndexConstant.LC_TABLE_DESC_RANGE_STR));
    this.tracker = tracker;
    Path dirPath = indexParameters.getTmpDirPath(hdfsTmpPath);
    if (indexParameters.getLCIndexFileSystem().exists(dirPath)) {
      indexParameters.getLCIndexFileSystem().mkdirs(dirPath);
    }
  }

  /**
   * 1. parse kvQueue into clustering index
   * 2. write index files
   * 3. write statistics files
   */
  public void processKeyValueQueue(Queue<KeyValue> kvQueue) throws IOException {
    IndexPutParser indexPutParser =
        IndexPutParser.getParser(tableRelation.getIndexType(), tableRelation);
    byte[] prevRow = null;
    Put put = null;
    Map<TableName, List<Put>> putMap = new HashMap<>();
    // parse kvQueue into putMap
    while (!kvQueue.isEmpty()) {
      KeyValue kv = kvQueue.poll();
      //      LCIndexConstant.mWinterToString(kv);
      byte[] row = CellUtil.cloneRow(kv);
      if (prevRow == null || Bytes.compareTo(prevRow, row) != 0) { // a new put
        processPut(putMap, put, indexPutParser);
        put = new Put(row);
        prevRow = row;
      }
      put.add(kv);
      // update stat
      if (tableRelation.isIndexColumn(CellUtil.cloneFamily(kv), CellUtil.cloneQualifier(kv))) {
        TableName tableName =
            tableRelation.getIndexTableName(CellUtil.cloneFamily(kv), CellUtil.cloneQualifier(kv));
        LCStatInfo2 statInfo2 = statMap.get(tableName);
        if (statInfo2 != null) {
          statMap.get(tableName).updateRange(CellUtil.cloneValue(kv));
        }
      }
    }
    // the last put
    processPut(putMap, put, indexPutParser);
    // write puts
    for (Entry<TableName, List<Put>> entry : putMap.entrySet()) {
      writePuts(entry.getKey(), entry.getValue());
      writeStatFile(statMap.get(entry.getKey()));
    }
  }

  /**
   * parse put, add index put into putMap
   */
  private void processPut(Map<TableName, List<Put>> putMap, Put put, IndexPutParser indexPutParser)
      throws IOException {
    if (put == null) return;
    Map<TableName, Put> parsedPut = indexPutParser.parsePut(put);
    for (Entry<TableName, Put> entry : parsedPut.entrySet()) {
      List<Put> list = putMap.get(entry.getKey());
      if (list == null) {
        list = new ArrayList<>();
        putMap.put(entry.getKey(), list);
      }
      list.add(entry.getValue());
    }
  }

  /**
   * write key-value (as cells of puts) into local file
   */
  private void writePuts(TableName tableName, List<Put> puts) throws IOException {
    byte[] family = null;
    byte[] qualifier = null;
    for (Map.Entry<byte[], TreeSet<byte[]>> entry : tableRelation.getIndexFamilyMap().entrySet()) {
      for (byte[] q : entry.getValue()) {
        if (tableName.equals(tableRelation.getIndexTableName(entry.getKey(), q))) {
          family = entry.getKey();
          qualifier = q;
          break;
        }
      }
    }
    if (family == null) {
      throw new IOException("cannot find index column for index table: " + tableName);
    }
    StoreFile.Writer writer = createIndexWriter(family, qualifier, puts.size());
    List<KeyValue> kvList = new ArrayList<>();
    for (Put put : puts)
      for (List<Cell> cells : put.getFamilyCellMap().values())
        for (Cell cell : cells)
          kvList.add((KeyValue) cell);
    Collections.sort(kvList, KeyValue.COMPARATOR);
    for (Cell cell : kvList) {
      writer.append(cell);
    }
    LOG.info("flushing memstore to file: " + writer.getPath() + " with " + puts.size() + " cells");
    writer.close();
  }

  private StoreFile.Writer createIndexWriter(byte[] family, byte[] qualifier, int maxKeyCount)
      throws IOException {
    Path iFilePath = indexParameters.getTmpIFilePath(hdfsTmpPath, qualifier);
    StoreFile.Writer writer = store
        .createTmpIFileWriter(maxKeyCount, store.family.getCompression(), false, true, false, false,
            iFilePath);
    if (tracker != null) writer.setTimeRangeTracker(tracker);
    return writer;
  }

  private void writeStatFile(LCStatInfo2 stat) throws IOException {
    Path sFilePath = indexParameters.getTmpSFilePath(hdfsTmpPath, stat.getQualifier());
    BufferedWriter bw = new BufferedWriter(
        new OutputStreamWriter(indexParameters.getLCIndexFileSystem().create(sFilePath, true)));
    if (stat.isSet()) {
      for (long l : stat.getSetValues()) {
        bw.write(l + "\n");
      }
    } else {
      for (long l : stat.getCounts()) {
        bw.write(l + "\n");
      }
    }
    LOG.info("flushing stat info to file: " + sFilePath);
    bw.close();
  }
}
