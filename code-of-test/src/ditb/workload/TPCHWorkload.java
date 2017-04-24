package ditb.workload;

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
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * Created by winter on 17-3-15.
 */
public class TPCHWorkload extends DataGeneratedWorkload {

  public static final String[] columnNames =
      new String[] { "ck", "st", "t", "d", "p", "cl", "sh", "cm" };
  public static final String[] indexColumnNames = new String[] { "t", "d", "p" };

  int maxTotalPrice = Integer.MIN_VALUE, minTotalPrice = Integer.MAX_VALUE;
  int maxDate = Integer.MIN_VALUE, minDate = Integer.MAX_VALUE;
  int maxPriority = Integer.MIN_VALUE, minPriority = Integer.MAX_VALUE;

  public TPCHWorkload(String descFilePath) throws IOException {
    super(descFilePath);
  }

  @Override public IndexTableRelation getTableRelation(TableName tableName, IndexType indexType)
      throws IOException {
    IndexTableRelation relation = new IndexTableRelation(tableName, indexType);
    for (String col : indexColumnNames) {
      relation.addIndexColumn(FAMILY_NAME, Bytes.toBytes(col));
    }
    for (String col : columnNames) {
      relation.addColumn(FAMILY_NAME, Bytes.toBytes(col));
    }
    return relation;
  }

  @Override public byte[][] getSplits() {
    if (nbRegion <= 1) return null;
    byte[][] splits = new byte[nbRegion][];
    byte[] right = Bytes.toBytes((long) 0);
    for (int i = 0; i < nbRegion; i++) {
      splits[i] = Bytes.add(Bytes.toBytes(i), right);
    }
    return splits;
  }

  @Override public List<String> statDescriptions() {
    List<String> list = new ArrayList<>();
    list.add(String
        .format("t\t%s\t%d\t%d\t%d", DataType.INT.toString(), minTotalPrice, maxTotalPrice,
            nbLCStatRange));
    list.add(String
        .format("d\t%s\t%d\t%d\t%d", DataType.INT.toString(), minDate, maxDate, nbLCStatRange));
    list.add(String.format("p\t%s\t%d\t%d\t%d", DataType.INT.toString(), minPriority, maxPriority,
        nbLCStatRange));
    return list;
  }

  @Override public AbstractDITBRecord loadDITBRecord(String str) throws ParseException {
    TPCHRecord record = new TPCHRecord(str);
    maxDate = Math.max(maxDate, record.getDate());
    minDate = Math.min(minDate, record.getDate());
    maxTotalPrice = Math.max(maxTotalPrice, record.getTotalPrice());
    minTotalPrice = Math.min(minTotalPrice, record.getTotalPrice());
    maxPriority = Math.max(maxPriority, record.getPriority());
    minPriority = Math.min(minPriority, record.getPriority());
    return record;
  }

  @Override public int getMDDimensions() {
    return 3;
  }

  @Override public MDRange[] getMDRanges(ScanRange.ScanRangeList rangeList) {
    MDRange[] ranges = new MDRange[3];
    for (ScanRange range : rangeList.getRanges()) {
      if (Bytes.toString(range.getQualifier()).equalsIgnoreCase("t")) {
        ranges[0] = DITBUtil.getMDRange(range);
      } else if (Bytes.toString(range.getQualifier()).equalsIgnoreCase("d")) {
        ranges[1] = DITBUtil.getMDRange(range);
      } else if (Bytes.toString(range.getQualifier()).equalsIgnoreCase("p")) {
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

  @Override public String parseResult(Result result) {
    byte[] rowkey = result.getRow();
    // aId, uId, cId
    long orderKey = Bytes.toLong(rowkey, 4);
    long custKey = Bytes.toLong(result.getValue(FAMILY_NAME, Bytes.toBytes("ck")));
    String status = Bytes.toString(result.getValue(FAMILY_NAME, Bytes.toBytes("st")));
    int totalPrice = Bytes.toInt(result.getValue(FAMILY_NAME, Bytes.toBytes("t")));
    int date = Bytes.toInt(result.getValue(FAMILY_NAME, Bytes.toBytes("d")));
    int priority = Bytes.toInt(result.getValue(FAMILY_NAME, Bytes.toBytes("p")));
    String clerk = Bytes.toString(result.getValue(FAMILY_NAME, Bytes.toBytes("cl")));
    int shipPriority = Bytes.toInt(result.getValue(FAMILY_NAME, Bytes.toBytes("sh")));
    String comment = Bytes.toString(result.getValue(FAMILY_NAME, Bytes.toBytes("cm")));
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    sb.append("orderKey=").append(orderKey).append(",");
    sb.append("custKey=").append(custKey).append(",");
    sb.append("status=").append(status).append(",");
    sb.append("totalPrice=").append(totalPrice).append(",");
    sb.append("date=").append(date).append(",");
    sb.append("priority=").append(priority).append(",");
    sb.append("clerk=").append(clerk).append(",");
    sb.append("shipPriority=").append(shipPriority).append(",");
    sb.append("comment=").append(comment).append("}");
    return sb.toString();
  }

  class TPCHRecord extends AbstractDITBRecord {
    private final String DELIMITER = "|";

    private String line;
    private long orderKey;
    private long custKey;
    private String status;
    private int totalPrice;
    private int date;
    private int priority;
    private String clerk;
    private int shipPriority;
    private String comment;

    private final DateFormat from = new SimpleDateFormat("yyyy-MM-dd");
    private final DateFormat to = new SimpleDateFormat("yyyyMMdd");

    public TPCHRecord(String str) throws ParseException {
      this.line = str;
      int currentPos = 0;
      String temp;
      int index = 0;
      while (true) {
        index = str.indexOf(DELIMITER);
        if (index < 0) break;
        temp = str.substring(0, index);
        str = str.substring(index + 1);
        switch (currentPos) {
        case 0:
          orderKey = Long.valueOf(temp);
          break;
        case 1:
          custKey = Long.valueOf(temp);
          break;
        case 2:
          status = temp;
          break;
        case 3:
          totalPrice = totalPriceToInt(temp);
          break;
        case 4:
          // date
          String s = to.format(from.parse(temp));
          date = Integer.valueOf(s);
          break;
        case 5:
          priority = priorityToInt(temp);
          break;
        case 6:
          clerk = temp;
          break;
        case 7:
          shipPriority = Integer.valueOf(temp);
          break;
        case 8:
          comment = temp;
          break;
        default:
        }
        ++currentPos;
      }
    }

    public int getDate() {
      return date;
    }

    public int getTotalPrice() {
      return totalPrice;
    }

    public int getPriority() {
      return priority;
    }

    private int totalPriceToInt(String price) {
      double p = Double.valueOf(price) * 100;
      if (p > Integer.MAX_VALUE * 1.0) {
        throw new RuntimeException("price out of scope: " + price);
      }
      return (int) p;
    }

    private int priorityToInt(String priority) {
      if (priority.equals("1-URGENT")) return 1;
      if (priority.equals("2-HIGH")) return 2;
      if (priority.equals("3-MEDIUM")) return 3;
      if (priority.equals("4-NOT SPECIFIED")) return 4;
      if (priority.equals("5-LOW")) return 5;
      throw new RuntimeException("known priority: " + priority);
    }

    private byte[] getSaltRowkey() {
      int saltValue = Long.hashCode(orderKey) % nbRegion;
      return Bytes.add(Bytes.toBytes(saltValue), Bytes.toBytes(orderKey));
    }

    @Override public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(orderKey).append(DELIMITER);
      sb.append(custKey).append(DELIMITER);
      sb.append(status).append(DELIMITER);
      sb.append(totalPrice).append(DELIMITER);
      sb.append(date).append(DELIMITER);
      sb.append(priority).append(DELIMITER);
      sb.append(clerk).append(DELIMITER);
      sb.append(shipPriority).append(DELIMITER);
      sb.append(comment).append(DELIMITER);
      return sb.toString();
    }

    @Override public byte[] getRowkey() {
      return getSaltRowkey();
    }

    @Override public Put getPut() {
      Put put = new Put(getSaltRowkey());
      put.addColumn(FAMILY_NAME, Bytes.toBytes("ck"), Bytes.toBytes(custKey));
      put.addColumn(FAMILY_NAME, Bytes.toBytes("st"), Bytes.toBytes(status));
      put.addColumn(FAMILY_NAME, Bytes.toBytes("t"), Bytes.toBytes(totalPrice));
      put.addColumn(FAMILY_NAME, Bytes.toBytes("d"), Bytes.toBytes(date));
      put.addColumn(FAMILY_NAME, Bytes.toBytes("p"), Bytes.toBytes(priority));
      put.addColumn(FAMILY_NAME, Bytes.toBytes("cl"), Bytes.toBytes(clerk));
      put.addColumn(FAMILY_NAME, Bytes.toBytes("sh"), Bytes.toBytes(shipPriority));
      put.addColumn(FAMILY_NAME, Bytes.toBytes("cm"), Bytes.toBytes(comment));
      return put;
    }

    @Override public MDPoint toMDPoint() {
      int[] arr = new int[] { totalPrice, date, priority };
      return new MDPoint(getSaltRowkey(), arr);
    }

    @Override public String toLine() {
      return line;
    }
  }
}
