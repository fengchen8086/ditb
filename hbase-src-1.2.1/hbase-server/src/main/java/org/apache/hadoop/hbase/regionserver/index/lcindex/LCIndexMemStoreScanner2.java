package org.apache.hadoop.hbase.regionserver.index.lcindex;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.index.IndexType;
import org.apache.hadoop.hbase.index.scanner.ScanRange;
import org.apache.hadoop.hbase.index.userdefine.IndexPutParser;
import org.apache.hadoop.hbase.index.userdefine.IndexTableRelation;
import org.apache.hadoop.hbase.regionserver.NonLazyKeyValueScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.mortbay.log.Log;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

// winter newly added class
public class LCIndexMemStoreScanner2 extends NonLazyKeyValueScanner {

  ArrayList<Cell> dataList;
  int currentIndexPoint;

  public LCIndexMemStoreScanner2(RegionScanner regionScanner, IndexTableRelation relation,
      ScanRange primaryRange) throws IOException {
    super();
    long start = System.currentTimeMillis();
    IndexTableRelation copyRelation =
        new IndexTableRelation(relation.getTableName(), IndexType.LCIndex);
    copyRelation.addIndexColumn(primaryRange.getFamily(), primaryRange.getQualifier());
    copyRelation.setColumns(relation.getFamilyMap());
    IndexPutParser parser = IndexPutParser.getParser(IndexType.LCIndex, copyRelation);
    dataList = init(regionScanner, primaryRange, parser);
    currentIndexPoint = 0;
    System.out.println("LCDBG, LCIndexScanner cost " + (System.currentTimeMillis() - start) / 1000.0
        + " seconds to build LCIndexMemstoreScanner2 from memstore, the size of this scanner is: "
        + dataList.size());
  }

  private ArrayList<Cell> init(RegionScanner regionScanner, ScanRange primaryRange,
      IndexPutParser parse) throws IOException {
    ArrayList<Cell> ret = new ArrayList<>();
    while (true) {
      List<Cell> oneRow = new ArrayList<>();
      if (!regionScanner.nextRaw(oneRow)) break;
      processOneRow(ret, oneRow, parse);
      //      processOneRow(ret, oneRow, primaryRange, relation);
    }
    Collections.sort(ret, KeyValue.COMPARATOR);
    return ret;
  }

  private void processOneRow(ArrayList<Cell> ret, List<Cell> oneRow, IndexPutParser parse)
      throws IOException {
    if (oneRow == null || oneRow.isEmpty()) return;
    Put put = new Put(oneRow.get(0).getRow());
    for (Cell cell : oneRow)
      put.add(cell);
    for (Put newPut : parse.parsePut(put).values()) {
      for (List<Cell> cellList : newPut.getFamilyCellMap().values())
        ret.addAll(cellList);
    }
  }

  /**
   * build clustering index for cells in {oneRow}, primary column is referred by {primaryRange}.
   * all generated cells are added in {ret}
   */
  private void processOneRow(ArrayList<Cell> ret, List<Cell> oneRow, ScanRange primaryRange,
      IndexTableRelation relation) throws IOException {
    if (oneRow == null) return;
    // find the primary cell
    Cell primaryCell = null;
    for (Cell cell : oneRow) {
      if (Bytes.equals(CellUtil.cloneQualifier(cell), primaryRange.getQualifier()) && Bytes
          .equals(CellUtil.cloneFamily(cell), primaryRange.getFamily())) {
        primaryCell = cell;
        break;
      }
    }
    if (primaryCell == null) throw new IOException(
        "should not happen because row after filter does not contain target column");

    byte[] indexedKey = IndexPutParser
        .getIndexRow(CellUtil.cloneRow(primaryCell), CellUtil.cloneValue(primaryCell));
    for (Cell cell : oneRow) {
      if (cell != primaryCell) {
        KeyValue kv =
            new KeyValue(indexedKey, CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell),
                cell.getTimestamp(), KeyValue.Type.codeToType(cell.getTypeByte()),
                CellUtil.cloneValue(cell));
        //        , cell.getTagsArray());
        //        System.out.println(
        //            "in LCIndexMemStoreScanner2 raw: " + LCIndexConstant.mWinterToString(cell)
        //                + ", after clustering: " + LCIndexConstant.mWinterIndexedToString(kv));
        ret.add(kv);
      }
    }
  }

  @Override public synchronized Cell peek() {
    if (currentIndexPoint >= dataList.size()) {
      return null;
    }
    return dataList.get(currentIndexPoint);
  }

  @Override public synchronized Cell next() {
    if (currentIndexPoint >= dataList.size()) {
      return null;
    }
    final Cell ret = dataList.get(currentIndexPoint);
    ++currentIndexPoint;
    return ret;
  }

  /**
   * Set the scanner at the seek key. Must be called only once: there is no thread safety between
   * the scanner and the memStore.
   *
   * @param key seek value
   * @return false if the key is null or if there is no data
   */
  @Override public synchronized boolean seek(Cell key) {
    if (key == null) {
      close();
      return false;
    }
    //    Log.info("LCDBG, WARN, here we use key.getRow() instead of key.getKey()");
    // if the current key is greater than the target, return false
    if (currentIndexPoint >= dataList.size()
        || Bytes.compareTo(key.getRow(), ((KeyValue) (dataList.get(currentIndexPoint))).getRow())
        <= 0) {
      if (currentIndexPoint < dataList.size()) {
        System.out.println(
            "LCIndex memstore scanner return false after seek!, key: " + cellToString(key)
                + ", cur: " + cellToString(dataList.get(currentIndexPoint)));
      }
      return false;
    }

    System.out.println("key: " + Bytes.toString(key.getRow()) + ", peek: " + Bytes
        .toString(dataList.get(currentIndexPoint).getRow()));

    while (currentIndexPoint < dataList.size()) {
      // the key now pointed should be right bigger than the target
      // return the first point where the value is greater than the target
      if (Bytes.compareTo(key.getRow(), dataList.get(currentIndexPoint).getRow()) <= 0) {
        return true;
      }
      ++currentIndexPoint;
    }
    return false;
  }

  private String cellToString(Cell cell) {
    return LCIndexConstant.mWinterIndexedToString(cell) + ", with row: " + Bytes
        .toStringBinary(CellUtil.cloneRow(cell));
  }

  /**
   * Move forward on the sub-lists set previously by seek.
   *
   * @param key seek value (should be non-null)
   * @return true if there is at least one KV to read, false otherwise
   */
  @Override public synchronized boolean reseek(Cell key) {
    if (key == null) {
      close();
      return false;
    }
    // if the target is greater than the max, return false
    if (dataList.size() == 0
        || Bytes.compareTo(key.getRow(), dataList.get(dataList.size() - 1).getRow()) > 0) {
      return false;
    }
    currentIndexPoint = 0;
    while (currentIndexPoint < dataList.size()) {
      // the key now pointed should be right bigger than the target
      // return the first point where the value is greater than the target
      // int cmp = Bytes.compareTo(key.getKey(), dataList.get(currentIndexPoint).getKey());
      if (KeyValue.COMPARATOR.compare(key, dataList.get(currentIndexPoint)) <= 0) {
        return true;
      }
      ++currentIndexPoint;
    }
    return false;
  }

  @Override public long getSequenceID() {
    return Long.MAX_VALUE - 1;
  }

  public synchronized void close() {
    dataList.clear();
    dataList = null;
    currentIndexPoint = 0;
  }

  /**
   * Seek scanner to the given key first. If it returns false(means peek()==null) or scanner's peek
   * row is bigger than row of given key, seek the scanner to the previous row of given key
   */
  @Override public synchronized boolean backwardSeek(Cell key) {
    Log.info("LCWARN, method backwardSeek is not implemented!");
    return true;
  }

  /**
   * Separately get the KeyValue before the specified key from kvset and snapshotset, and use the
   * row of higher one as the previous row of specified key, then seek to the first KeyValue of
   * previous row
   */
  @Override public synchronized boolean seekToPreviousRow(Cell key) {
    Log.info("LCWARN, method seekToPreviousRow is not implemented!");
    return true;
  }

  @Override public synchronized boolean seekToLastRow() {
    Log.info("LCWARN, method seekToLastRow is not implemented!");
    return true;
  }

}
