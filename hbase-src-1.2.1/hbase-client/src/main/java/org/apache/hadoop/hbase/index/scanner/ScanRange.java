package org.apache.hadoop.hbase.index.scanner;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.index.client.DataType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by winter on 16-11-28.
 */
public class ScanRange implements Writable {

  public static final String SCAN_RANGE_ATTRIBUTE_STR = "scan_range_attr";
  private final byte[] ROWKEY_APPENDER = Bytes.toBytes("#");
  private byte[] family;
  private byte[] qualifier;
  private byte[] start;
  private byte[] stop;
  private CompareFilter.CompareOp startOp;
  private CompareFilter.CompareOp stopOp;
  private long startTs;
  private long stopTs;
  private DataType dataType;

  public ScanRange() {

  }

  public ScanRange(byte[] family, byte[] qualifier, byte[] start, byte[] stop,
      CompareFilter.CompareOp startOp, CompareFilter.CompareOp stopOp, long startTs, long stopTs,
      DataType dataType) {
    this.family = family;
    this.qualifier = qualifier;
    this.start = start;
    this.stop = stop;
    this.startOp = startOp;
    this.stopOp = stopOp;
    this.startTs = startTs;
    this.stopTs = stopTs;
    this.dataType = dataType;
  }

  public ScanRange(byte[] family, byte[] qualifier, byte[] start, byte[] stop,
      CompareFilter.CompareOp startOp, CompareFilter.CompareOp stopOp, DataType dataType) {
    this(family, qualifier, start, stop, startOp, stopOp, -1, -1, dataType);
  }

  public ScanRange(byte[] family, byte[] qualifier, byte[] start, byte[] stop,
      CompareFilter.CompareOp startOp, CompareFilter.CompareOp stopOp) {
    this(family, qualifier, start, stop, startOp, stopOp, -1, -1, DataType.STRING);
  }

  public byte[] getQualifier() {
    return qualifier;
  }

  public byte[] getFamily() {
    return family;
  }

  public byte[] getStart() {
    return start;
  }

  public byte[] getStop() {
    if (startOp == CompareFilter.CompareOp.EQUAL) {
      return Bytes.add(start, ROWKEY_APPENDER);
    }
    if (stop != null) {
      if (stopOp == CompareFilter.CompareOp.LESS_OR_EQUAL) {
        return Bytes.add(stop, ROWKEY_APPENDER);
      }
    }
    return stop;
  }

  public byte[] getRawStop() {
    return stop;
  }

  public long getStartTs() {
    return startTs;
  }

  public long getStopTs() {
    return stopTs;
  }

  @Override public void write(DataOutput out) throws IOException {
    Bytes.writeByteArray(out, family);
    Bytes.writeByteArray(out, qualifier);
    // start
    if (start == null) {
      Bytes.writeByteArray(out, Bytes.toBytes(true));
    } else {
      Bytes.writeByteArray(out, Bytes.toBytes(false));
      Bytes.writeByteArray(out, start);
      Bytes.writeByteArray(out, Bytes.toBytes(startOp.toString()));
    }
    // stop
    if (stop == null) {
      Bytes.writeByteArray(out, Bytes.toBytes(true));
    } else {
      Bytes.writeByteArray(out, Bytes.toBytes(false));
      Bytes.writeByteArray(out, stop);
      Bytes.writeByteArray(out, Bytes.toBytes(stopOp.toString()));
    }
    out.writeLong(startTs);
    out.writeLong(stopTs);
    Bytes.writeByteArray(out, Bytes.toBytes(dataType.toString()));
  }

  @Override public void readFields(DataInput in) throws IOException {
    family = Bytes.readByteArray(in);
    qualifier = Bytes.readByteArray(in);
    boolean startNull = Bytes.toBoolean(Bytes.readByteArray(in));
    if (startNull) {
      start = null;
      startOp = CompareFilter.CompareOp.NO_OP;
    } else {
      start = Bytes.readByteArray(in);
      startOp = CompareFilter.CompareOp.valueOf(Bytes.toString(Bytes.readByteArray(in)));
    }
    boolean stopNull = Bytes.toBoolean(Bytes.readByteArray(in));
    if (stopNull) {
      stop = null;
      stopOp = CompareFilter.CompareOp.NO_OP;
    } else {
      stop = Bytes.readByteArray(in);
      stopOp = CompareFilter.CompareOp.valueOf(Bytes.toString(Bytes.readByteArray(in)));
    }
    startTs = in.readLong();
    stopTs = in.readLong();
    dataType = DataType.valueOf(Bytes.toString(Bytes.readByteArray(in)));
  }

  @Override public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[").append(Bytes.toString(family)).append(":").append(Bytes.toString(qualifier))
        .append("]");
    sb.append(DataType.byteToString(dataType, start)).append("-").append(startOp).append(", ");
    sb.append(DataType.byteToString(dataType, stop)).append("-").append(stopOp);
    return sb.toString();
  }

  public DataType getDataType() {
    return dataType;
  }

  public CompareFilter.CompareOp getStartOp() {
    return startOp;
  }

  public CompareFilter.CompareOp getStopOp() {
    return stopOp;
  }

  public static class ScanRangeList implements Writable {
    private List<ScanRange> ranges;

    public ScanRangeList() {
      ranges = new ArrayList<>();
    }

    public ScanRangeList(List<ScanRange> list) {
      ranges = list;
    }

    public void addScanRange(ScanRange scanRange) {
      ranges.add(scanRange);
    }

    @Override public void write(DataOutput out) throws IOException {
      out.writeInt(ranges.size());
      for (ScanRange sr : ranges)
        sr.write(out);
    }

    @Override public void readFields(DataInput in) throws IOException {
      int size = in.readInt();
      for (int i = 0; i < size; i++) {
        ScanRange sr = new ScanRange();
        sr.readFields(in);
        ranges.add(sr);
      }
    }

    public static ScanRangeList getScanRangeList(Scan scan) throws IOException {
      ScanRangeList rangeList = new ScanRange.ScanRangeList();
      byte[] attr = scan.getAttribute(ScanRange.SCAN_RANGE_ATTRIBUTE_STR);
      if (attr == null) {
        throw new IOException("scan should have attribute " + ScanRange.SCAN_RANGE_ATTRIBUTE_STR
            + " to convert to ScanRangeList");
      }
      ByteArrayInputStream inputStream = new ByteArrayInputStream(attr);
      DataInputStream dis = new DataInputStream(inputStream);
      rangeList.readFields(dis);
      return rangeList;
    }

    public static Scan getScan(String fileName) throws IOException {
      BufferedReader br = new BufferedReader(new FileReader(fileName));
      String line;
      Scan scan = new Scan();
      System.out.println("winter for scan ******");
      FilterList filterList = new FilterList();
      while ((line = br.readLine()) != null) {
        System.out.println("winter for scan : " + line);
        if (line.startsWith("#")) continue;
        // family, qualifier, type, >=, 10, <=, 1000
        // family, qualifier, type, >=, 10
        String[] splits = line.split("\t");
        byte[] family = Bytes.toBytes(splits[0]);
        byte[] qualifier = Bytes.toBytes(splits[1]);
        DataType type = DataType.valueOf(splits[2].toUpperCase());
        CompareFilter.CompareOp firstOp = parseOp(splits[3]);
        byte[] firstValue = DataType.stringToBytes(type, splits[4]);
        filterList.addFilter(new SingleColumnValueFilter(family, qualifier, firstOp, firstValue));
        if (splits.length >= 6) {
          CompareFilter.CompareOp secondOp = parseOp(splits[5].toUpperCase());
          byte[] secondValue = DataType.stringToBytes(type, splits[6]);
          filterList
              .addFilter(new SingleColumnValueFilter(family, qualifier, secondOp, secondValue));
        }
      }
      scan.setFilter(filterList);
      ScanRangeList scanRangeList = ScanRangeList.getScanRangeList(fileName);
      if (scanRangeList.getRanges().size() > 0) {
        scan.setAttribute(ScanRange.SCAN_RANGE_ATTRIBUTE_STR, scanRangeList.toBytesAttribute());
      }
      return scan;
    }

    public static ScanRangeList getScanRangeList(String fileName) throws IOException {
      ScanRangeList rangeList = new ScanRangeList();
      BufferedReader br = new BufferedReader(new FileReader(fileName));
      String line;
      while ((line = br.readLine()) != null) {
        if (line.startsWith("#")) continue;
        // family, qualifier, type, >=, 10, <=, 1000
        // family, qualifier, type, >=, 10
        String[] splits = line.split("\t");
        byte[] family = Bytes.toBytes(splits[0]);
        byte[] qualifier = Bytes.toBytes(splits[1]);
        DataType type = DataType.valueOf(splits[2].toUpperCase());
        CompareFilter.CompareOp firstOp = parseOp(splits[3]);
        byte[] firstValue = DataType.stringToBytes(type, splits[4]);
        switch (firstOp) {
        case EQUAL:
        case NOT_EQUAL:
          rangeList.addScanRange(new ScanRange(family, qualifier, firstValue, null, firstOp,
              CompareFilter.CompareOp.NO_OP, type));
          break;
        case LESS:
        case LESS_OR_EQUAL:
          rangeList.addScanRange(
              new ScanRange(family, qualifier, null, firstValue, CompareFilter.CompareOp.NO_OP,
                  firstOp, type));
          break;
        case GREATER:
        case GREATER_OR_EQUAL:
          CompareFilter.CompareOp secondOp = CompareFilter.CompareOp.NO_OP;
          byte[] secondValue = null;
          if (splits.length >= 6) {
            secondOp = parseOp(splits[5].toUpperCase());
            secondValue = DataType.stringToBytes(type, splits[6]);
          }
          rangeList.addScanRange(
              new ScanRange(family, qualifier, firstValue, secondValue, firstOp, secondOp, type));
          break;
        case NO_OP:
          break;
        }
      }
      return rangeList;
    }

    private static CompareFilter.CompareOp parseOp(String str) {
      if ("==".equalsIgnoreCase(str) || "=".equalsIgnoreCase(str))
        return CompareFilter.CompareOp.EQUAL;
      if (">".equalsIgnoreCase(str)) return CompareFilter.CompareOp.GREATER;
      if (">=".equalsIgnoreCase(str)) return CompareFilter.CompareOp.GREATER_OR_EQUAL;
      if ("<".equalsIgnoreCase(str)) return CompareFilter.CompareOp.LESS;
      if ("<=".equalsIgnoreCase(str)) return CompareFilter.CompareOp.LESS_OR_EQUAL;
      if ("!=".equalsIgnoreCase(str)) return CompareFilter.CompareOp.NOT_EQUAL;
      return CompareFilter.CompareOp.NO_OP;
    }

    public List<ScanRange> getRanges() {
      return ranges;
    }

    public FilterList toFilterList() {
      return toFilterList(null);
    }

    public FilterList toFilterList(ScanRange exclude) {
      if (ranges.isEmpty()) return null;
      FilterList filterList = new FilterList();
      for (ScanRange range : ranges) {
        if (range == exclude) continue;
        if (range.getStartOp() != CompareFilter.CompareOp.NO_OP) {
          //          System.out.println("start filter: " + range.getStartOp() + " on qualifier " + Bytes
          //              .toString(range.getQualifier()) + " with value: " + DataType
          //              .byteToString(range.dataType, range.getStart()));
          SingleColumnValueFilter f =
              new SingleColumnValueFilter(range.getFamily(), range.getQualifier(),
                  range.getStartOp(), range.getStart());
          //          f.setLatestVersionOnly(false);
          filterList.addFilter(f);
        }
        if (range.getStopOp() != CompareFilter.CompareOp.NO_OP) {
          //          System.out.println("stop filter: " + range.getStopOp() + " on qualifier " + Bytes
          //              .toString(range.getQualifier()) + " with value: " + DataType
          //              .byteToString(range.dataType, range.getStop()));
          SingleColumnValueFilter f =
              new SingleColumnValueFilter(range.getFamily(), range.getQualifier(),
                  range.getStopOp(), range.getStop());
          //          f.setLatestVersionOnly(false);
          filterList.addFilter(f);
        }
      }
      //      System.out.println("final filterList.size()=" + filterList.getFilters().size());
      return filterList;
    }

    @Override public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("{");
      for (ScanRange range : ranges) {
        sb.append(range);
      }
      sb.append("}");
      return sb.toString();
    }

    public byte[] toBytesAttribute() {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(baos);
      try {
        this.write(dos);
        dos.flush();
      } catch (IOException e) {
        e.printStackTrace();
      }
      return baos.toByteArray();
    }
  }
}

