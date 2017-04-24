package org.apache.hadoop.hbase.index.scanner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.index.IndexType;
import org.apache.hadoop.hbase.index.userdefine.IllegalIndexException;
import org.apache.hadoop.hbase.index.userdefine.IndexPutParser;
import org.apache.hadoop.hbase.index.userdefine.IndexTableRelation;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by winter on 16-11-25.
 */
public abstract class BaseIndexScanner implements ResultScanner {
  protected static final Log LOG = LogFactory.getLog(BaseIndexScanner.class);
  private static Map<IndexType, Class> supportedScanners;
  protected long totalScanTime = 0;
  protected long totalNumberOfRecords = 0;

  static {
    supportedScanners = new HashMap<>();
    supportedScanners.put(IndexType.NoIndex, NoIndexScanner.class);
    supportedScanners.put(IndexType.GSIndex, GSScanner.class);
    supportedScanners.put(IndexType.GCIndex, GCScanner.class);
    supportedScanners.put(IndexType.CCIndex, GCScanner.class);
    supportedScanners.put(IndexType.IRIndex, IRScanner.class);
    supportedScanners.put(IndexType.LSIndex, LocalScanner.class);
    supportedScanners.put(IndexType.LCIndex, LocalScanner.class);
    supportedScanners.put(IndexType.UDGIndex, UDGScanner.class);
    supportedScanners.put(IndexType.UDLIndex, LocalScanner.class);
    supportedScanners.put(IndexType.LMDIndex_S, LocalScanner.class);
    supportedScanners.put(IndexType.LMDIndex_D, LocalScanner.class);
  }

  protected Connection conn;
  protected IndexTableRelation relation;
  protected ScanRange.ScanRangeList rangeList;
  protected Scan rawScan;

  public BaseIndexScanner(Connection conn, IndexTableRelation relation, Scan scan)
      throws IOException {
    this.conn = conn;
    this.relation = relation;
    this.rawScan = scan;
    rangeList = ScanRange.ScanRangeList.getScanRangeList(scan);
  }

  public BaseIndexScanner(BaseIndexScanner base) {
    this.conn = base.conn;
    this.relation = base.relation;
    this.rawScan = base.rawScan;
    this.rangeList = base.rangeList;
  }

  public static BaseIndexScanner getIndexScanner(Connection conn, IndexTableRelation relation,
      Scan scan) throws IllegalIndexException {
    Class cls = supportedScanners.get(relation.getIndexType());
    if (cls == null) {
      throw new IllegalIndexException(relation.getTableName(), relation.getIndexType(),
          "index scanner not exists");
    }
    BaseIndexScanner instance = null;
    try {
      Class[] paraTypes = new Class[] { Connection.class, IndexTableRelation.class, Scan.class };
      Object[] params = new Object[] { conn, relation, scan };
      Constructor con = cls.getConstructor(paraTypes);
      instance = (BaseIndexScanner) con.newInstance(params);
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
      throw new IllegalIndexException(relation.getTableName(), relation.getIndexType(),
          "meet error in creating instance of " + cls.getName());
    }
    return instance;
  }

  /**
   * count covering regions for [start, end], used in clustering index
   *
   * @param tableName
   * @param start
   * @param end
   * @return
   * @throws IOException
   */
  protected static int countCoveringRegions(Connection conn, TableName tableName, byte[] start,
      byte[] end) throws IOException {
    RegionLocator locator = conn.getRegionLocator(tableName);
    List<HRegionLocation> list = locator.getAllRegionLocations();
    localTest(list);
    int left = start == null ? 0 : lookupRegionIndex(list, start);
    int right = end == null ? list.size() - 1 : lookupRegionIndex(list, end);
    return right - left + 1;
  }

  protected static int lookupRegionIndex(List<HRegionLocation> list, byte[] key) {
    if (list.size() == 1 || Bytes.compareTo(key, list.get(0).getRegionInfo().getStartKey()) <= 0)
      return 0;
    if (Bytes.compareTo(key, list.get(list.size() - 1).getRegionInfo().getStartKey()) >= 0)
      return list.size() - 1;
    int l = 0, r = list.size() - 1;
    while (l < r) {
      int mid = (l + r) / 2;
      int cmp = Bytes.compareTo(key, list.get(mid).getRegionInfo().getStartKey());
      if (cmp == 0) {
        return mid;
      } else if (cmp > 0) {
        if (Bytes.compareTo(key, list.get(mid + 1).getRegionInfo().getStartKey()) < 0) return mid;
        l = mid + 1;
      } else {
        r = mid - 1;
      }
    }
    return l;
  }

  private static void localTest(List<HRegionLocation> list) throws IOException {
    byte[] prev = null;
    for (HRegionLocation rl : list) {
      if (prev != null) {
        if (Bytes.compareTo(prev, rl.getRegionInfo().getStartKey()) >= 0) {
          throw new IOException("region locations are not ordered at all!");
        }
      }
      prev = rl.getRegionInfo().getStartKey();
    }
  }

  public abstract void printScanLatencyStatistic();

  @Override public Iterator<Result> iterator() {
    return new BaseIndexScannerIterator(this);
  }

  public class BaseIndexScannerIterator implements Iterator<Result> {
    Result nextResult = null;
    BaseIndexScanner scanner;

    public BaseIndexScannerIterator(BaseIndexScanner scanner) {
      this.scanner = scanner;
      seekNext();
    }

    @Override public boolean hasNext() {
      return nextResult != null;
    }

    @Override public Result next() {
      Result ret = nextResult;
      seekNext();
      return ret;
    }

    @Override public void remove() {
      LOG.info("cannot remove elements in " + this.getClass().getName());
    }

    private void seekNext() {
      try {
        nextResult = scanner.next();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public static Result recoverClusteringResult(Result raw, byte[] family, byte[] qualifier) {
    if (raw == null) return null;
    byte[][] indexColumn = IndexPutParser.parseIndexRowKey(raw.getRow());
    List<KeyValue> list = new ArrayList<>(raw.listCells().size() + 1);
    for (Cell cell : raw.listCells()) {
      byte[] tag = cell.getTagsArray();
      if (tag != null && tag.length > KeyValue.MAX_TAGS_LENGTH) tag = null;
      KeyValue kv =
          new KeyValue(indexColumn[0], CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell),
              cell.getTimestamp(), KeyValue.Type.codeToType(cell.getTypeByte()),
              CellUtil.cloneValue(cell), tag);
      list.add(kv);
    }
    list.add(new KeyValue(indexColumn[0], family, qualifier, indexColumn[1]));
    Collections.sort(list, KeyValue.COMPARATOR);
    return new Result(list);
  }

  public static List<Cell> recoverClusteringResult(List<Cell> cells, byte[] family,
      byte[] qualifier) {
    if (cells == null || cells.size() == 0) return cells;
    byte[][] indexColumn = IndexPutParser.parseIndexRowKey(cells.get(0).getRow());
    List<Cell> list = new ArrayList<>(cells.size() + 1);
    for (Cell cell : cells) {
      byte[] tag = cell.getTagsArray();
      if (tag != null && tag.length > KeyValue.MAX_TAGS_LENGTH) tag = null;
      KeyValue kv =
          new KeyValue(indexColumn[0], CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell),
              cell.getTimestamp(), KeyValue.Type.codeToType(cell.getTypeByte()),
              CellUtil.cloneValue(cell), tag);
      list.add(kv);
    }
    list.add(new KeyValue(indexColumn[0], family, qualifier, indexColumn[1]));
    Collections.sort(list, KeyValue.COMPARATOR);
    return list;
  }

}