package userdefined;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.index.IndexType;
import org.apache.hadoop.hbase.index.client.DataType;
import org.apache.hadoop.hbase.index.scanner.BaseIndexScanner;
import org.apache.hadoop.hbase.index.scanner.ScanRange;
import org.apache.hadoop.hbase.index.userdefine.ColumnInfo;
import org.apache.hadoop.hbase.index.userdefine.IndexRelationship;
import org.apache.hadoop.hbase.index.userdefine.IndexTableAdmin;
import org.apache.hadoop.hbase.index.userdefine.IndexTableRelation;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.index.lcindex.LCStatInfo2;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Created by winter on 16-11-19.
 */
public class BaseRunner {
  protected static List<IndexRelationship> indexRelationships;
  private final long SLEEP_TIME = 10 * 1000;

  protected final IndexType indexType;
  protected IndexTableAdmin admin;
  protected Connection conn;
  protected Configuration configuration;
  protected TreeMap<byte[], TreeSet<byte[]>> columnFamily;
  protected TreeMap<byte[], TreeSet<byte[]>> indexColumnFamily;
  protected TreeMap<byte[], RangeDescription> rangeMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
  protected ScanRange.ScanRangeList scanRangeList = new ScanRange.ScanRangeList();
  private int rowCounter = 1;
  StringBuilder lcindexRangeDesc = new StringBuilder();
  private TreeSet<byte[]> notPrintingSet = new TreeSet<>(Bytes.BYTES_COMPARATOR);
  protected TableName tableName;

  public BaseRunner(IndexType indexType, String tableDescFile) throws IOException {
    System.out.println("Running BaseRunner version 0.04");
    this.indexType = indexType;
    tableName = TableName.valueOf("ud-tbl-" + indexType);
    parseColumns(tableDescFile);
  }

  private void parseColumns(String tableDescFile) throws IOException {
    if (!new File(tableDescFile).exists())
      throw new IOException("table description file not exists: " + tableDescFile);
    try (BufferedReader br = new BufferedReader(new FileReader(tableDescFile))) {
      String line;
      while ((line = br.readLine()) != null) {
        if (line.startsWith("#") || line.trim().length() == 0) continue;
        System.out.println("parsing table stat line: " + line);
        String[] splits = line.split(" ");
        byte[] family = Bytes.toBytes(splits[1]);
        byte[] qualifier = Bytes.toBytes(splits[2]);
        byte[] key = Bytes.add(family, qualifier);
        if (splits[0].equalsIgnoreCase("column")) {
          if (columnFamily == null) columnFamily = new TreeMap<>(Bytes.BYTES_COMPARATOR);
          TreeSet<byte[]> set = columnFamily.get(family);
          if (set == null) {
            set = new TreeSet<>(Bytes.BYTES_COMPARATOR);
            columnFamily.put(family, set);
          }
          set.add(qualifier);
          RangeDescription rangeDesc;
          if ("set".equalsIgnoreCase(splits[4])) {
            rangeDesc =
                new RangeDescription(family, qualifier, DataType.valueOf(splits[3]), splits, 5);
          } else if ("random".equalsIgnoreCase(splits[4])) {
            rangeDesc = new RangeDescription(family, qualifier, DataType.valueOf(splits[3]),
                Integer.valueOf(splits[5]));
            notPrintingSet.add(qualifier);
          } else {
            rangeDesc =
                new RangeDescription(family, qualifier, DataType.valueOf(splits[3]), splits[4],
                    splits[5]);
          }
          rangeMap.put(key, rangeDesc);
        } else if (splits[0].equalsIgnoreCase("index-column")) {
          if (IndexType.isUserDefinedIndex(indexType)) continue;
          if (indexColumnFamily == null) indexColumnFamily = new TreeMap<>(Bytes.BYTES_COMPARATOR);
          TreeSet<byte[]> set = indexColumnFamily.get(family);
          if (set == null) {
            set = new TreeSet<>(Bytes.BYTES_COMPARATOR);
            indexColumnFamily.put(family, set);
          }
          set.add(qualifier);
          if (indexType == IndexType.LCIndex) {
            lcindexRangeDesc.append(Bytes.toString(family)).append("\t")
                .append(Bytes.toString(qualifier));
            for (int startIdx = 3; startIdx < splits.length; startIdx++) {
              lcindexRangeDesc.append("\t").append(splits[startIdx]);
            }
            lcindexRangeDesc.append(LCStatInfo2.LC_TABLE_DESC_RANGE_DELIMITER);
          }
        } else if (splits[0].equalsIgnoreCase("user-defined-index")) {
          if (!IndexType.isUserDefinedIndex(indexType)) continue;
          ColumnInfo mainCol = new ColumnInfo(family, qualifier);
          List<ColumnInfo> list = new ArrayList<>();
          for (int i = 3; i < splits.length; i += 2)
            list.add(new ColumnInfo(Bytes.toBytes(splits[i]), Bytes.toBytes(splits[i + 1])));
          if (indexRelationships == null) indexRelationships = new ArrayList<>();
          indexRelationships.add(new IndexRelationship(mainCol, list));
        } else if (splits[0].equalsIgnoreCase("scan-range")) {
          byte[] start = DataType.stringToBytes(DataType.valueOf(splits[3]), splits[5]);
          byte[] stop = splits.length >= 8 ?
              DataType.stringToBytes(DataType.valueOf(splits[3]), splits[7]) :
              null;
          CompareFilter.CompareOp startOp = CompareFilter.CompareOp.NO_OP;
          CompareFilter.CompareOp stopOp = CompareFilter.CompareOp.NO_OP;
          if (splits[4].equals("=")) {
            startOp = CompareFilter.CompareOp.EQUAL;
          } else if (splits[4].equals("<=") || splits[4].equals("<")) {
            stopOp = splits[4].equals("<=") ?
                CompareFilter.CompareOp.LESS_OR_EQUAL :
                CompareFilter.CompareOp.LESS;
          } else {
            startOp = splits[4].equals(">=") ?
                CompareFilter.CompareOp.GREATER_OR_EQUAL :
                CompareFilter.CompareOp.GREATER;
            if (splits.length >= 8) stopOp = splits[6].equals("<=") ?
                CompareFilter.CompareOp.LESS_OR_EQUAL :
                CompareFilter.CompareOp.LESS;
          }
          scanRangeList.addScanRange(new ScanRange(family, qualifier, start, stop, startOp, stopOp,
              DataType.valueOf(splits[3])));
        } else {
          throw new IOException("cannot parse line: " + line + " in file: " + tableDescFile);
        }
      }
    }
  }

  protected String getLCIndexRangeStr() {
    return lcindexRangeDesc.toString();
  }

  protected String resultToString(Result result) {
    StringBuilder sb = new StringBuilder();
    sb.append("{").append(keyToString(result.getRow())).append(":");
    for (Cell cell : result.listCells()) {
      byte[] f = CellUtil.cloneFamily(cell);
      byte[] q = CellUtil.cloneQualifier(cell);
      RangeDescription range = rangeMap.get(Bytes.add(f, q));
      sb.append("[").append(Bytes.toString(f)).append(":").append(Bytes.toString(q)).append("->");
      if (notPrintingSet.contains(q)) sb.append("skipped random value");
      else sb.append(DataType.byteToString(range.dataType, CellUtil.cloneValue(cell)));
      sb.append("]");
    }
    sb.append("}");
    return sb.toString();
  }

  public void init(String additionalConf) throws IOException {
    configuration = HBaseConfiguration.create();
    HRegionServer.loadWinterConf(configuration, additionalConf);
    conn = ConnectionFactory.createConnection(configuration);
    IndexTableRelation relation;
    if (IndexType.isUserDefinedIndex(indexType))
      relation = getUserDefinedIndexTableRelation(tableName, indexType);
    else relation = getRegularIndexTableRelation(tableName, indexType);
    admin = new IndexTableAdmin(configuration, conn, relation);
    if (indexType == IndexType.LCIndex) admin.setLCIndexRange(getLCIndexRangeStr());
    //    admin.createTable(false, false);

    byte[][] splits = new byte[10][];
    for (int i = 0; i < 10; i++) {
      splits[i] = Bytes.toBytes(i * 1000);
    }
    admin.createTable(true, true, splits);
  }

  private int REPORT_SIZE = 1000;

  public void insertData(int recordNumber) throws IOException {
    for (int i = 0; i < recordNumber; i++) {
      List<Put> puts = generateNextRows(1);
      admin.putWithIndex(puts);
      if (i % REPORT_SIZE == 0) System.out.println("LCDBG, " + i + " records has been written");
    }
    if (indexType != IndexType.LCIndex) return;
    System.out.println("sleep " + SLEEP_TIME + "ms before scan");
    try {
      Thread.sleep(SLEEP_TIME);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public void queryData(boolean printResult, String outputFile) throws IOException {
    Scan scan = getScan();
    BaseIndexScanner scanner = admin.getScanner(scan);
    int counter = 0;
    Result result;
    BufferedWriter bw = null;
    if (outputFile != null && !outputFile.equalsIgnoreCase("null") && outputFile.length() > 0) {
      bw = new BufferedWriter(new FileWriter(outputFile));
    }
    while ((result = scanner.next()) != null) {
      String str = resultToString(result);
      if (printResult) System.out.println("print result:" + str);
      if (bw != null) bw.write(str + "\n");
      ++counter;
    }
    bw.close();
    System.out.println("totally get " + counter + " results for scan, print to: " + outputFile);
  }

  public void report() throws IOException {
  }

  public void close() throws IOException {
    admin.close();
    if (conn != null) conn.close();
  }

  public static byte[] keyToBytes(int key) {
    return Bytes.toBytes(key);
  }

  public static String keyToString(byte[] key) {
    return String.valueOf(Bytes.toInt(key));
  }

  protected TreeMap<byte[], TreeSet<byte[]>> getIndexColumnFamily() {
    return indexColumnFamily;
  }

  protected TreeMap<byte[], TreeSet<byte[]>> getColumnFamily() {
    return columnFamily;
  }

  protected List<IndexRelationship> getIndexRelationships() {
    return indexRelationships;
  }

  protected IndexTableRelation getRegularIndexTableRelation(TableName tableName,
      IndexType indexType) throws IOException {
    IndexTableRelation relation = new IndexTableRelation(tableName, indexType);
    relation.setColumns(getColumnFamily());
    relation.setIndexColumns(getIndexColumnFamily());
    return relation;
  }

  protected IndexTableRelation getUserDefinedIndexTableRelation(TableName tableName,
      IndexType indexType) throws IOException {
    IndexTableRelation relation = new IndexTableRelation(tableName, indexType);
    relation.setColumns(getColumnFamily());
    relation.setIndexRelations(getIndexRelationships());
    return relation;
  }

  /**
   * generate data for all columns, with build-in counter
   *
   * @return
   */
  public Put generateNextRow() {
    Put put = new Put(keyToBytes(rowCounter));
    for (RangeDescription rd : rangeMap.values()) {
      put.addColumn(rd.family, rd.qualifier, rd.getValue(rowCounter));
    }
    ++rowCounter;
    return put;
  }

  public List<Put> generateNextRows(int n) {
    List<Put> list = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      list.add(generateNextRow());
    }
    return list;
  }

  protected Scan getScan() {
    Scan scan = new Scan();
    scan.setAttribute(ScanRange.SCAN_RANGE_ATTRIBUTE_STR, scanRangeList.toBytesAttribute());
    scan.setFilter(scanRangeList.toFilterList());
    scan.setCacheBlocks(false);
    return scan;
  }

  private class RangeDescription {
    final byte[] family;
    final byte[] qualifier;
    final DataType dataType;
    boolean isSet = false;
    boolean isRandomString = false;
    String min;
    String max;
    List<String> list = null;
    int randomLength;

    private RangeDescription(byte[] family, byte[] qualifier, DataType dataType) {
      this.family = family;
      this.qualifier = qualifier;
      this.dataType = dataType;
    }

    public RangeDescription(byte[] family, byte[] qualifier, DataType dataType, String min,
        String max) {
      this(family, qualifier, dataType);
      this.min = min;
      this.max = max;
    }

    public RangeDescription(byte[] family, byte[] qualifier, DataType dataType, String[] parts,
        int startIndex) {
      this(family, qualifier, dataType);
      isSet = true;
      list = new ArrayList<>();
      for (int i = startIndex; i < parts.length; i++) {
        list.add(parts[i]);
      }
    }

    public RangeDescription(byte[] family, byte[] qualifier, DataType dataType, int length) {
      this(family, qualifier, dataType);
      isRandomString = true;
      randomLength = length;
    }

    public byte[] getValue(int key) {
      if (isSet) return DataType.stringToBytes(dataType, list.get(key % list.size()));
      else if (isRandomString) return Bytes.toBytes(getRandomString(randomLength));
      else return DataType.stringToBytes(dataType, String.valueOf(key));
    }
  }

  public static String getRandomString(int length) {
    StringBuffer buffer =
        new StringBuffer("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ");
    StringBuilder sb = new StringBuilder(length);
    Random random = new Random();
    int range = buffer.length();
    for (int i = 0; i < length; i++) {
      sb.append(buffer.charAt(random.nextInt(range)));
    }
    return sb.toString();
  }
}
