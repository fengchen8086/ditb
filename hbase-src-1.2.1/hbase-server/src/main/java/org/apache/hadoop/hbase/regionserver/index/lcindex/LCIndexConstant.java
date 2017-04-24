package org.apache.hadoop.hbase.regionserver.index.lcindex;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.index.client.DataType;
import org.apache.hadoop.hbase.index.client.Range;
import org.apache.hadoop.hbase.index.userdefine.IndexPutParser;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.TreeMap;

public class LCIndexConstant {

  // lcc index generator
  public static final String DELIMITER_STR = "#";
  public static final byte[] DELIMITER_BYTES = Bytes.toBytes(DELIMITER_STR);
  public static final String DELIMITER_PLUS_ONE_STR = "$";
  public static final byte[] DELIMITER_PLUS_ONE_BYTES = Bytes.toBytes(DELIMITER_PLUS_ONE_STR);
  public static final String CF_FAMILY_PREFIX_STR = "if_";
  public static final byte[] CF_FAMILY_PREFIX_BYTES = Bytes.toBytes(CF_FAMILY_PREFIX_STR);
  public static final String TABLE_DESC_FORMAT_STR = "LC_DATA_FORMAT";
  public static final String LC_TABLE_DESC_RANGE_STR = "LC_DATA_RANGE";
  public static final String ROWKEY_RANGE = "ROWKEY_RANGE";

  public static final String LC_STAT_FILE_SUFFIX = "-stat";

  // file path
  public static final String LCINDEX_DIR_NAME = ".lcindex";
  public static final String LCINDEX_TMP_DIR_NAME = ".lctmp";
  public static final String INDEX_DIR_NAME_DEBUG = ".debuglcc";
  public static final String SCAN_WITH_LCCINDEX = "scan.with.lccindex";
  public static final String LCINDEX_LOCAL_DIR = "lcindex.local.dir";
  public static final String LCINDEX_REGIONSERVER_HOSTNAMES = "lcindex.regionserver.hostnames";
  public static final String LCINDEX_REGIONSERVER_HOSTNAMES_DELIMITER = ",";
  public static final String LCINDEX_USE_LOCAL_FS = "lcindex.index.write.local";

  // lcc hfile mover
  public static final String LCC_MOVER_PORT = "lcindex.mover.port";
  public static final int DEFAULT_LCC_MOVER_PORT = 63036;
  public static final String SLEEP_TIME_WHEN_QUEUE_EMPTY = "lcindex.sleep.when.queue.empty";
  public static final long DEFAULT_SLEEP_TIME_WHEN_QUEUE_EMPTY = 10 * 1000;
  public static final String LCC_MOVER_BUFFER_LEN = "lcindex.mover.buffer.length";
  public static final int DEFAULT_LCC_MOVER_BUFFER_LEN = 1024 * 1024;
  public static final String LCC_LOCAL_FILE_FOUND_MSG = "FILE_FOUND_MSG";
  public static final String LCC_LOCAL_FILE_NOT_FOUND_MSG = "FILE_NOT_FOUND_MSG";
  public static final String LCC_LOCAL_FILE_NOT_FILE_MSG = "FILE_NOT_A_FILE_MSG";
  public static final String TCP_BYE_MSG = "SEE_YOU_COFFEY";
  public static final String DELETE_HEAD_MSG = "DELETE_HEAD_MSG";
  public static final String REQUIRE_HEAD_MSG = "REQUIRE_HEAD_MSG";
  public static final String DELETE_SUCCESS_MSG = "DELETE_FILE_SUCCESS_MSG";
  public static final String NO_MEANING_MSG = "PLACE_HOLDER_MSG";
  public static final String FILE_IN_COMMIT_QUEUE_MSG = "FILE_IN_COMMIT_QUEUE_MSG";
  public static final String FILE_IN_COMPLETECOMPACTION_QUEUE_MSG =
      "FILE_IN_COMPLETECOMPACTION_QUEUE_MSG";

  // format
  public static final String FORMAT_INT_STR = "%012d";
  public static final String FORMAT_DOUBLE_STR = "%.2f";
  public static final String FORMAT_ZERO_STR = "0";

  public static final int LCCINDEX_PREFIX_ROWKEY_LENGTH = 15;

  public static String paddedStringInt(int i) {
    return StringUtils
        .leftPad(String.format(FORMAT_INT_STR, i), LCCINDEX_PREFIX_ROWKEY_LENGTH, FORMAT_ZERO_STR);
    // return String.valueOf(i);
  }

  public static String paddedStringLong(long d) {
    return StringUtils
        .leftPad(String.format(FORMAT_INT_STR, d), LCCINDEX_PREFIX_ROWKEY_LENGTH, FORMAT_ZERO_STR);
    // return String.valueOf(d);
  }

  public static String paddedStringShort(short d) {
    return StringUtils
        .leftPad(String.format(FORMAT_INT_STR, d), LCCINDEX_PREFIX_ROWKEY_LENGTH, FORMAT_ZERO_STR);
    // return String.valueOf(b);
  }

  public static String paddedStringDouble(double d) {
    return StringUtils.leftPad(String.format(FORMAT_DOUBLE_STR, d), LCCINDEX_PREFIX_ROWKEY_LENGTH,
        FORMAT_ZERO_STR);
    // return String.valueOf(d);
  }

  // public static String paddedStringString(String s) {
  // return StringUtils.leftPad(s, LCCINDEX_PREFIX_ROWKEY_LENGTH, FORMAT_ZERO_STR);
  // }

  public static DataType getType(TreeMap<byte[], DataType> map, byte[] qualifier) {
    assert map != null;
    return qualifier == null ? DataType.STRING : map.get(qualifier);
  }

  public static String getStringOfValueAndType(final DataType type, final byte[] data) {
    if (data == null) return "null";
    if (type == DataType.SHORT || type == DataType.INT) {
      return String.valueOf(Bytes.toInt(data));
    }
    if (type == DataType.DOUBLE) {
      return String.valueOf(Bytes.toDouble(data));
    }
    if (type == DataType.LONG) {
      return String.valueOf(Bytes.toLong(data));
    }
    if (type == DataType.STRING) {
      return Bytes.toString(data);
    }
    return "mWinterGetStringOfValueAndType type not supported!";
  }

  public static String getStringOfValue(TreeMap<byte[], DataType> map, final byte[] qualifier,
      final byte[] data) {
    return getStringOfValueAndType(getType(map, qualifier), data);
  }

  public static int compareValues(byte[] b1, byte[] b2, DataType type) {
    switch (type) {
    case INT:
      // jdk 1.7
      // return Integer.compare(Bytes.toInt(b1), Bytes.toInt(b2));
      // jdk 1.6
      return compareInt(Bytes.toInt(b1), Bytes.toInt(b2));
    case LONG:
      return compareLong(Bytes.toLong(b1), Bytes.toLong(b2));
    case DOUBLE:
      return Double.compare(Bytes.toDouble(b1), Bytes.toDouble(b2));
    case STRING:
      return Bytes.toString(b1).compareTo(Bytes.toString(b2));
    default:
      break;
    }
    new Exception("winter compareWithQualifier not supportted type: " + type).printStackTrace();
    return 0;
  }

  public static int compareWithQualifier(TreeMap<byte[], DataType> map, final byte[] qualifier,
      byte[] b1, byte[] b2) {
    return compareValues(b1, b2, getType(map, qualifier));
  }

  public static byte[] parsingStringToBytes(TreeMap<byte[], DataType> map, final byte[] qualifier,
      String s) {
    return parsingStringToBytesWithType(getType(map, qualifier), s);
  }

  public static byte[] parsingStringToBytesWithType(DataType type, String s) {
    switch (type) {
    case INT:
      return Bytes.toBytes(Integer.valueOf(s));
    case DOUBLE:
      return Bytes.toBytes(Double.valueOf(s));
    case LONG:
      return Bytes.toBytes(Long.valueOf(s));
    case SHORT:
      return Bytes.toBytes(Short.valueOf(s));
    case BOOLEAN:
      return Bytes.toBytes(Boolean.valueOf(s));
    case STRING:
      return Bytes.toBytes(s);
    }
    return null;
  }

  public static String convertUnknownBytes(byte[] bytes) {
    if (bytes != null && bytes.length >= 2) {
      int length = bytes[1];
      byte[] bb = new byte[length];
      for (int i = 0; i < length; ++i) {
        bb[i] = bytes[i + 2];
      }
      return Bytes.toString(bb);
    } else {
      return "unknown bytes";
    }
  }

  private static class UNSHEED_CF_INFO {
    protected String qualifier;
    protected DataType type;
    protected boolean isIndex;

    public UNSHEED_CF_INFO(String s, DataType type, boolean isIndex) {
      qualifier = s;
      this.type = type;
      this.isIndex = isIndex;
    }
  }

  private static ArrayList<UNSHEED_CF_INFO> cfs = new ArrayList<UNSHEED_CF_INFO>();

  static {
    // basic test
    cfs.add(new UNSHEED_CF_INFO("d", DataType.DOUBLE, true));
    cfs.add(new UNSHEED_CF_INFO("i", DataType.INT, true));
    cfs.add(new UNSHEED_CF_INFO("s", DataType.STRING, true));
    cfs.add(new UNSHEED_CF_INFO("INFO", DataType.STRING, false));
    // tpc-h test
    //    cfs.add(new UNSHEED_CF_INFO("ok", DataType.LONG, false));
    //    cfs.add(new UNSHEED_CF_INFO("ck", DataType.LONG, false));
    //    cfs.add(new UNSHEED_CF_INFO("st", DataType.STRING, false));
    //    cfs.add(new UNSHEED_CF_INFO("t", DataType.DOUBLE, true));
    //    cfs.add(new UNSHEED_CF_INFO("d", DataType.LONG, true));
    //    cfs.add(new UNSHEED_CF_INFO("p", DataType.STRING, true));
    //    cfs.add(new UNSHEED_CF_INFO("cl", DataType.STRING, false));
    //    cfs.add(new UNSHEED_CF_INFO("sh", DataType.INT, false));
    //    cfs.add(new UNSHEED_CF_INFO("cm", DataType.STRING, false));
  }

  public static String mWinterToString(Cell kv) {
    return mWinterToString((KeyValue) kv);
  }

  public static String mWinterIndexedtoStringWithBinary(Cell cell) {
    return LCIndexConstant.mWinterIndexedToString(cell) + ", with row: " + Bytes
        .toStringBinary(CellUtil.cloneRow(cell));
  }

  public static String mWinterToString(KeyValue kv) {
    if (kv == null) {
      return "**KeyValue is empty!**";
    }
    String valueStr = null;
    for (UNSHEED_CF_INFO ci : cfs) {
      if (ci.qualifier.equals(Bytes.toString(kv.getQualifier()))) {
        valueStr = LCIndexConstant.getStringOfValueAndType(ci.type, kv.getValue());
        break;
      }
    }
    if (valueStr == null) {
      valueStr = Bytes.toString(kv.getValue());
    }
    if ("Info".equalsIgnoreCase(Bytes.toString(kv.getQualifier()))) {
      valueStr = "info too long, hide";
    }
    return Bytes.toInt(kv.getRow()) + "[" + Bytes.toString(kv.getFamily()) + ":" + Bytes
        .toString(kv.getQualifier()) + "]" + valueStr;
  }

  public static String mWinterIndexedToString(Cell kv) {
    if (kv == null) {
      return "**KeyValue is empty!**";
    }
    String valueStr = null;
    String head = "unknown head";
    for (UNSHEED_CF_INFO ci : cfs) {
      if (ci.qualifier.equals(Bytes.toString(kv.getQualifier()))) {
        valueStr = LCIndexConstant.getStringOfValueAndType(ci.type, kv.getValue());
        byte[][] parsed = IndexPutParser.parseIndexRowKey(kv.getRow());
        head = String.valueOf(Bytes.toInt(parsed[0]));
        break;
      }
    }
    if (valueStr == null) {
      valueStr = Bytes.toString(kv.getValue());
    }
    if ("Info".equalsIgnoreCase(Bytes.toString(kv.getQualifier()))) {
      valueStr = "info too long, hide";
    }
    return head + "[" + Bytes.toString(kv.getFamily()) + ":" + Bytes.toString(kv.getQualifier())
        + "]" + valueStr;
  }

  private static int compareInt(int a, int b) {
    if (a < b) return -1;
    if (a > b) return 1;
    return 0;
  }

  private static int compareLong(long a, long b) {
    if (a < b) return -1;
    if (a > b) return 1;
    return 0;
  }

  public static String printRange(Range r) {
    StringBuilder sb = new StringBuilder();
    sb.append("[" + Bytes.toString(r.getFamily()) + ":" + Bytes.toString(r.getQualifier())
        + "], values (");
    if (r.getStartValue() != null) {
      sb.append(LCIndexConstant.getStringOfValueAndType(r.getDataType(), r.getStartValue()));
      if (r.getStartType() == CompareOp.EQUAL || r.getStartType() == CompareOp.NOT_EQUAL) {
        sb.append(" <== ").append(r.getStartType()).append(" )");
        return sb.toString();
      }
    } else {
      sb.append("null");
    }
    sb.append(", ");
    if (r.getStopValue() != null) {
      sb.append(LCIndexConstant.getStringOfValueAndType(r.getDataType(), r.getStopValue()));
    } else {
      sb.append("MAX");
    }
    sb.append(")");
    return sb.toString();
  }
}
