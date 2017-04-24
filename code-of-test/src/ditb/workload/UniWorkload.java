package ditb.workload;

import ditb.util.DITBConstants;
import ditb.util.DITBUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.index.IndexType;
import org.apache.hadoop.hbase.index.client.DataType;
import org.apache.hadoop.hbase.index.mdhbase.MDPoint;
import org.apache.hadoop.hbase.index.mdhbase.MDRange;
import org.apache.hadoop.hbase.index.scanner.ScanRange;
import org.apache.hadoop.hbase.index.userdefine.IndexTableRelation;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by winter on 17-3-7.
 */
public class UniWorkload extends RandomGeneratedWorkload {

  // table desc
  public final String[] indexColumnNames = new String[] { "a", "b", "c" }; // see UniDBITRecord
  // data column definition from workload desc File
  private int maxIndexColumnValue;

  int maxA = Integer.MIN_VALUE, minA = Integer.MAX_VALUE;
  int maxB = Integer.MIN_VALUE, minB = Integer.MAX_VALUE;
  int maxC = Integer.MIN_VALUE, minC = Integer.MAX_VALUE;

  public UniWorkload(String descFilePath) throws IOException {
    super(descFilePath);
    maxIndexColumnValue = loadIntParam("max.index.column.value", 10000 * 10000);
  }

  @Override public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("This is ").append(this.getClass().getName());
    sb.append(super.toString());
    sb.append(", nbTotalColumns=").append(nbTotalColumns);
    sb.append(", nbTotalOperations=").append(nbTotalOperations);
    sb.append(", maxIndexColumnValue=").append(maxIndexColumnValue);
    return sb.toString();
  }

  @Override public AbstractDITBRecord loadDITBRecord(String str) throws ParseException {
    return new DITBUniRecord(str);
  }

  @Override public AbstractDITBRecord geneDITBRecord(Random random) {
    return geneDITBRecord(Math.abs(random.nextInt()), random);
  }

  @Override public AbstractDITBRecord geneDITBRecord(int rowkey, Random random) {
    DITBUniRecord record = new DITBUniRecord(rowkey, random.nextInt(maxIndexColumnValue),
        random.nextInt(maxIndexColumnValue), random.nextInt(maxIndexColumnValue), random);
    maxA = Math.max(maxA, record.getA());
    minA = Math.min(minA, record.getA());
    maxB = Math.max(maxB, record.getB());
    minB = Math.min(minB, record.getB());
    maxC = Math.max(maxC, record.getC());
    minC = Math.min(minC, record.getC());
    return record;
  }

  @Override public List<String> statDescriptions() {
    List<String> list = new ArrayList();
    list.add(
        String.format("a\t%s\t%d\t%d\t%d", DataType.INT.toString(), minA, maxA, nbLCStatRange));
    list.add(
        String.format("b\t%s\t%d\t%d\t%d", DataType.INT.toString(), minB, maxB, nbLCStatRange));
    list.add(
        String.format("c\t%s\t%d\t%d\t%d", DataType.INT.toString(), minC, maxC, nbLCStatRange));
    return list;
  }

  public IndexTableRelation getTableRelation(TableName tableName, IndexType indexType)
      throws IOException {
    IndexTableRelation relation = new IndexTableRelation(tableName, indexType);
    for (String col : indexColumnNames) {
      relation.addIndexColumn(FAMILY_NAME, Bytes.toBytes(col));
      relation.addColumn(FAMILY_NAME, Bytes.toBytes(col));
    }
    for (int i = DITBUniRecord.DATA_COLUMN_OFFSET; i < nbTotalColumns; i++) {
      relation.addColumn(FAMILY_NAME, Bytes.toBytes(i));
    }
    return relation;
  }

  public byte[][] getSplits() {
    if (nbRegion <= 1) return null;
    byte[][] splits = new byte[nbRegion][];
    byte[] right = Bytes.toBytes(0);
    for (int i = 0; i < nbRegion; i++) {
      splits[i] = Bytes.add(Bytes.toBytes(i), right);
    }
    return splits;
  }

  private byte[] getSaltRowkeyWithNbRegion(int rowkey) {
    int saltValue = new Integer(rowkey).hashCode() % nbRegion;
    return Bytes.add(Bytes.toBytes(saltValue), Bytes.toBytes(rowkey));
  }

  @Override public byte[] getRowkey(String rowkey) {
    return getSaltRowkeyWithNbRegion(Integer.valueOf(rowkey));
  }

  @Override public MDRange[] getMDRanges(ScanRange.ScanRangeList rangeList) {
    MDRange[] ranges = new MDRange[3];
    for (ScanRange range : rangeList.getRanges()) {
      if (Bytes.toString(range.getQualifier()).equalsIgnoreCase("a")) {
        ranges[0] = DITBUtil.getMDRange(range);
      } else if (Bytes.toString(range.getQualifier()).equalsIgnoreCase("b")) {
        ranges[1] = DITBUtil.getMDRange(range);
      } else if (Bytes.toString(range.getQualifier()).equalsIgnoreCase("c")) {
        ranges[2] = DITBUtil.getMDRange(range);
      }
    }
    for (int i = 0; i < 3; i++) {
      if (ranges[i] == null) {
        ranges[i] = new MDRange(0, Integer.MAX_VALUE);
      }
    }
    return ranges;
  }

  @Override public int getMDDimensions() {
    return 3;
  }

  private class DITBUniRecord extends AbstractDITBRecord {
    private final int rowkey;
    private final int a;
    private final int b;
    private final int c;
    private static final int DATA_COLUMN_OFFSET = 4;
    private final int[] columnValues; // [0, DATA_COLUMN_OFFSET-1] is not accessible

    public DITBUniRecord(String str) throws ParseException {
      String[] splits = str.split(DITBConstants.DELIMITER);
      rowkey = Integer.valueOf(splits[0]);
      a = Integer.valueOf(splits[1]);
      b = Integer.valueOf(splits[2]);
      c = Integer.valueOf(splits[3]);
      columnValues = new int[nbTotalColumns];
      for (int i = DATA_COLUMN_OFFSET; i < nbTotalColumns; i++) {
        columnValues[i] = Integer.valueOf(splits[i]);
      }
    }

    public DITBUniRecord(int rowkey, int a, int b, int c, Random random) {
      this.rowkey = rowkey;
      this.a = a;
      this.b = b;
      this.c = c;
      columnValues = new int[nbTotalColumns];
      for (int i = DATA_COLUMN_OFFSET; i < nbTotalColumns; i++) {
        columnValues[i] = random.nextInt();
      }
    }

    @Override public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(rowkey).append(DITBConstants.DELIMITER);
      sb.append(a).append(DITBConstants.DELIMITER);
      sb.append(b).append(DITBConstants.DELIMITER);
      sb.append(c).append(DITBConstants.DELIMITER);
      for (int i = DATA_COLUMN_OFFSET; i < columnValues.length; i++) {
        sb.append(i).append(DITBConstants.DELIMITER).append(columnValues[i])
            .append(DITBConstants.DELIMITER);
      }
      return sb.toString();
    }

    @Override public byte[] getRowkey() {
      return getSaltRowkey();
    }

    @Override public Put getPut() {
      Put put = new Put(getSaltRowkey());
      put.addColumn(FAMILY_NAME, Bytes.toBytes("a"), Bytes.toBytes(a));
      put.addColumn(FAMILY_NAME, Bytes.toBytes("b"), Bytes.toBytes(b));
      put.addColumn(FAMILY_NAME, Bytes.toBytes("c"), Bytes.toBytes(c));
      for (int i = DATA_COLUMN_OFFSET; i < nbTotalColumns; i++) {
        put.addColumn(FAMILY_NAME, Bytes.toBytes(i), Bytes.toBytes(columnValues[i]));
      }
      return put;
    }

    @Override public String toLine() {
      StringBuilder sb = new StringBuilder();
      sb.append(rowkey).append(DITBConstants.DELIMITER);
      sb.append(a).append(DITBConstants.DELIMITER);
      sb.append(b).append(DITBConstants.DELIMITER);
      sb.append(c).append(DITBConstants.DELIMITER);
      for (int i = DATA_COLUMN_OFFSET; i < columnValues.length; i++) {
        sb.append(columnValues[i]).append(DITBConstants.DELIMITER);
      }
      return sb.toString();
    }

    private byte[] getSaltRowkey() {
      return getSaltRowkeyWithNbRegion(rowkey);
    }

    @Override public MDPoint toMDPoint() {
      int[] arr = new int[] { a, b, c };
      return new MDPoint(getSaltRowkey(), arr);
    }

    /**
     * used by data generator
     *
     * @return
     */
    private int getA() {
      return a;
    }

    private int getB() {
      return b;
    }

    private int getC() {
      return c;
    }
  }

  @Override public String parseResult(Result result) {
    int rowkey = Bytes.toInt(result.getRow(), 4);
    int a = Bytes.toInt(result.getValue(FAMILY_NAME, Bytes.toBytes("a")));
    int b = Bytes.toInt(result.getValue(FAMILY_NAME, Bytes.toBytes("b")));
    int c = Bytes.toInt(result.getValue(FAMILY_NAME, Bytes.toBytes("c")));
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    sb.append("rowkey=").append(rowkey).append(",");
    sb.append("a=").append(a).append(",");
    sb.append("b=").append(b).append(",");
    sb.append("c=").append(c).append(",");
    sb.append("other ").append(nbTotalColumns - DITBUniRecord.DATA_COLUMN_OFFSET)
        .append(" data columns not shown");
    sb.append("}");
    return sb.toString();
  }
}
