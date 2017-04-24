package org.apache.hadoop.hbase.index.userdefine;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.index.IndexType;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Created by winter on 16-11-18.
 */
public abstract class IndexPutParser {
  private static Map<IndexType, Class> supportedPutParser;

  static {
    supportedPutParser = new HashMap<>();
    supportedPutParser.put(IndexType.GSIndex, SecondaryIndexParser.class);
    supportedPutParser.put(IndexType.GCIndex, ClusteringIndexParser.class);
    supportedPutParser.put(IndexType.CCIndex, ClusteringIndexParser.class);
    supportedPutParser.put(IndexType.IRIndex, SecondaryIndexParser.class);
    supportedPutParser.put(IndexType.LCIndex, ClusteringIndexParser.class);
    supportedPutParser.put(IndexType.UDGIndex, UserDefinedParser.class);
    supportedPutParser.put(IndexType.UDLIndex, UserDefinedParser.class);
  }

  protected Map<ColumnInfo, Set<ColumnInfo>> indexSourceMap;
  protected IndexType indexType;
  protected IndexTableRelation tableRelation;

  public IndexPutParser(IndexType iType, IndexTableRelation relation) {
    indexType = iType;
    tableRelation = relation;
    indexSourceMap = relation.getIndexColumnMap();
    //    printIndexSourceMap();
  }

  /**
   * create indexedKey
   *
   * @param row
   * @param value
   * @return
   */
  public static byte[] getIndexRow(byte[] row, byte[] value) {
    if (value == null || value.length == 0) {
      return null;
    }
    value = Bytes.add(value, UDConstants.MIN_ROW_KEY);
    byte[] a = Bytes.toBytes(UDConstants.LASTPART_ZERO
        .substring(0, UDConstants.LASTPART_LENGTH - ("" + value.length).length()) + value.length);
    return Bytes.add(value, row, a);
  }

  /**
   * parse the indexedKey into {rowKey, value}
   *
   * @param indexedKey
   * @return
   */
  public static byte[][] parseIndexRowKey(byte[] indexedKey) {
    int length =
        Integer.valueOf(Bytes.toString(Bytes.tail(indexedKey, UDConstants.LASTPART_LENGTH)))
            .intValue();
    byte[][] result = new byte[2][];
    // get row key of raw table
    result[0] = new byte[indexedKey.length - UDConstants.LASTPART_LENGTH - length];
    for (int i = 0; i < result[0].length; i++) {
      result[0][i] = indexedKey[i + length];
    }
    // get value of the index column
    result[1] = Bytes.head(indexedKey, length - 1);
    return result;
  }

  public static IndexPutParser getParser(IndexType indexType, IndexTableRelation relation)
      throws IllegalIndexException {
    if (indexType == IndexType.NoIndex) return null;
    Class cls = supportedPutParser.get(indexType);
    if (cls == null) {
      throw new IllegalIndexException(relation.getTableName(), indexType,
          "index put parser not exists");
    }
    IndexPutParser instance = null;
    try {
      Class[] paraTypes = new Class[] { IndexType.class, IndexTableRelation.class };
      Object[] params = new Object[] { indexType, relation };
      Constructor con = cls.getConstructor(paraTypes);
      instance = (IndexPutParser) con.newInstance(params);
    } catch (NoSuchMethodException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (InvocationTargetException e) {
      e.printStackTrace();
    }
    if (instance == null) {
      throw new IllegalIndexException(relation.getTableName(), indexType,
          "meet error in creating instance of " + cls.getName());
    }
    return instance;
  }

  private void printIndexSourceMap() {
    if (indexSourceMap == null) {
      System.out.println("LCDBG, print index source map null in index: " + indexType);
      return;
    }
    for (Map.Entry<ColumnInfo, Set<ColumnInfo>> entry : indexSourceMap.entrySet()) {
      StringBuilder sb = new StringBuilder();
      sb.append("LCDBG, print index source map: ");
      sb.append(entry.getKey()).append(" --> ");
      for (ColumnInfo one : entry.getValue()) {
        sb.append(one).append(",");
      }
      System.out.println(sb.toString());
    }
  }

  public Map<TableName, Put> parsePut(Put put) throws IOException {
    return parsePut(put, IndexType.isLocalIndex(tableRelation.getIndexType()));
  }

  protected abstract Map<TableName, Put> parsePut(Put put, boolean serverSide) throws IOException;

  protected Map<TableName, List<Put>> parsePutList(List<Put> puts, boolean serverSide)
      throws IOException {
    Map<TableName, List<Put>> ret = new HashMap<>();
    for (Put put : puts) { // parse puts one by one
      Map<TableName, Put> parsedMap = parsePut(put);
      for (Map.Entry<TableName, Put> entry : parsedMap.entrySet()) {
        List<Put> list = ret.get(entry.getKey());
        if (list == null) {
          list = new ArrayList<>();
          ret.put(entry.getKey(), list);
        }
        list.add(entry.getValue());
      }
    }
    return ret;
  }

  public Map<TableName, List<Put>> parsePutList(List<Put> puts) throws IOException {
    return parsePutList(puts, IndexType.isLocalIndex(tableRelation.getIndexType()));
  }

  public static class SecondaryIndexParser extends IndexPutParser {

    public SecondaryIndexParser(IndexType iType, IndexTableRelation relation) {
      super(iType, relation);
      // in secondary index, indexSourceMap is null and never used
    }

    @Override protected Map<TableName, Put> parsePut(Put put, boolean serverSide) {
      Map<TableName, Put> map = new HashMap<>();
      byte[] row = put.getRow();
      for (Map.Entry<byte[], List<Cell>> entry : put.getFamilyCellMap().entrySet()) {
        byte[] family = entry.getKey();
        for (Cell cell : entry.getValue()) {
          byte[] q = CellUtil.cloneQualifier(cell);
          if (tableRelation.isIndexColumn(family, q)) {
            TableName indexTableName = tableRelation.getIndexTableName(family, q);
            Put newPut = new Put(getIndexRow(row, CellUtil.cloneValue(cell)));
            if (serverSide) newPut
                .addColumn(IndexType.SEDONDARY_FAMILY_BYTES, (byte[]) null, cell.getTimestamp(),
                    null);
            else newPut.addColumn(IndexType.SEDONDARY_FAMILY_BYTES, null, null);
            map.put(indexTableName, newPut);
          }
        }
      }
      tableRelation.getIndexFamilyMap();
      return map;
    }

    /**
     * override this method to improve performance
     */
    @Override protected Map<TableName, List<Put>> parsePutList(List<Put> puts, boolean serverSide) {
      Map<TableName, List<Put>> map = new TreeMap<>();
      for (Put put : puts) {
        byte[] row = put.getRow();
        for (Map.Entry<byte[], List<Cell>> entry : put.getFamilyCellMap().entrySet()) {
          byte[] family = entry.getKey();
          for (Cell cell : entry.getValue()) {
            byte[] q = CellUtil.cloneQualifier(cell);
            if (tableRelation.isIndexColumn(family, q)) {
              TableName indexTableName = tableRelation.getIndexTableName(family, q);
              List<Put> list = map.get(indexTableName);
              if (list == null) {
                list = new ArrayList<>();
                map.put(indexTableName, list);
              }
              Put newPut = new Put(getIndexRow(row, CellUtil.cloneValue(cell)));
              if (serverSide) newPut
                  .addColumn(IndexType.SEDONDARY_FAMILY_BYTES, (byte[]) null, cell.getTimestamp(),
                      null);
              else newPut.addColumn(IndexType.SEDONDARY_FAMILY_BYTES, null, null);
              list.add(newPut);
            }
          }
        }
      }
      tableRelation.getIndexFamilyMap();
      return map;
    }
  }

  /**
   * Put Parser for Clustering Index, CCIndex is included
   */
  public static class ClusteringIndexParser extends IndexPutParser {

    public ClusteringIndexParser(IndexType iType, IndexTableRelation relation) {
      super(iType, relation);
    }

    @Override protected Map<TableName, Put> parsePut(Put put, boolean serverSide)
        throws IOException {
      Map<TableName, Put> map = new HashMap<>();
      byte[] row = put.getRow();
      // first loop, init all puts
      for (Map.Entry<byte[], List<Cell>> entry : put.getFamilyCellMap().entrySet()) {
        byte[] family = entry.getKey();
        for (Cell cell : entry.getValue()) {
          byte[] qualifier = CellUtil.cloneQualifier(cell);
          byte[] value = CellUtil.cloneValue(cell);
          if (tableRelation.isIndexColumn(family, qualifier)) {
            byte[] indexRow = getIndexRow(row, value);
            map.put(tableRelation.getIndexTableName(family, qualifier), new Put(indexRow));
            if (indexType == IndexType.CCIndex) {
              map.put(tableRelation.getCCTName(family, qualifier), new Put(indexRow));
            }
          }
        }
      }
      // second loop, fulfill the puts
      for (Map.Entry<byte[], List<Cell>> entry : put.getFamilyCellMap().entrySet()) {
        byte[] family = entry.getKey();
        for (Cell cell : entry.getValue()) {
          byte[] qualifier = CellUtil.cloneQualifier(cell);
          byte[] value = CellUtil.cloneValue(cell);
          ColumnInfo curCol = new ColumnInfo(family, qualifier);
          for (ColumnInfo srcInfo : indexSourceMap.get(curCol)) { // cell(ci) used in srcs
            Put newPut = map.get(tableRelation.getIndexTableName(srcInfo));
            if (serverSide) {
              newPut.addColumn(family, qualifier, cell.getTimestamp(), value);
            } else {
              newPut.addColumn(family, qualifier, value);
            }
            if (indexType == IndexType.CCIndex && tableRelation.isIndexColumn(curCol)
                && tableRelation.isIndexColumn(srcInfo)) {
              newPut = map.get(tableRelation.getCCTName(srcInfo));
              newPut.addColumn(family, qualifier, value); // CCIndex is client side
            }
          }
        }
      }
      return map;
    }
  }

  /**
   * parser for user defined index
   */
  public static class UserDefinedParser extends IndexPutParser {

    // mark all secondary column set to fulfill the empty columns
    Set<ColumnInfo> secondaryColumnSet;

    public UserDefinedParser(IndexType iType, IndexTableRelation relation) {
      super(iType, relation);
      secondaryColumnSet = new HashSet<>();
      for (IndexRelationship one : tableRelation.getIndexRelations()) {
        if (one.getAdditionalColumns().isEmpty()) {
          secondaryColumnSet.add(
              new ColumnInfo(one.getMainColumn().getFamily(), one.getMainColumn().getQualifier()));
        }
      }
    }

    @Override protected Map<TableName, Put> parsePut(Put put, boolean serverSide) {
      Map<TableName, Put> map = new HashMap<>();
      byte[] row = put.getRow();
      // first loop, init all puts, and add default column for secondary index
      for (Map.Entry<byte[], List<Cell>> entry : put.getFamilyCellMap().entrySet()) {
        byte[] family = entry.getKey();
        for (Cell cell : entry.getValue()) {
          byte[] qualifier = CellUtil.cloneQualifier(cell);
          byte[] value = CellUtil.cloneValue(cell);
          if (tableRelation.isIndexColumn(family, qualifier)) {
            byte[] indexRow = getIndexRow(row, value);
            Put newPut = new Put(indexRow);
            if (secondaryColumnSet.contains(new ColumnInfo(family, qualifier))) {
              if (serverSide) newPut
                  .addColumn(IndexType.SEDONDARY_FAMILY_BYTES, (byte[]) null, cell.getTimestamp(),
                      null);
              else newPut.addColumn(IndexType.SEDONDARY_FAMILY_BYTES, null, null);
            }
            map.put(tableRelation.getIndexTableName(family, qualifier), newPut);
          }
        }
      }
      // second loop, fulfill the puts
      for (Map.Entry<byte[], List<Cell>> entry : put.getFamilyCellMap().entrySet()) {
        byte[] family = entry.getKey();
        for (Cell cell : entry.getValue()) {
          byte[] qualifier = CellUtil.cloneQualifier(cell);
          byte[] value = CellUtil.cloneValue(cell);
          ColumnInfo curCol = new ColumnInfo(family, qualifier);
          Set<ColumnInfo> srcInfoSet = indexSourceMap.get(curCol);
          if (srcInfoSet != null) {
            // srcInfoSet is null for secondary index
            for (ColumnInfo srcInfo : srcInfoSet) { // cell(ci) used in srcs
              Put newPut = map.get(tableRelation.getIndexTableName(srcInfo));
              if (serverSide) newPut.addColumn(family, qualifier, cell.getTimestamp(), value);
              else newPut.addColumn(family, qualifier, value);
            }
          }
        }
      }
      return map;
    }
  }
}
