package org.apache.hadoop.hbase.regionserver.index.lcindex;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.index.client.DataType;
import org.apache.hadoop.hbase.index.userdefine.IndexTableRelation;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by winter on 16-12-5.
 */
public class LCStatInfo2 {
  private byte[] family;
  private byte[] qualifier;
  private DataType type;
  private boolean isSet;
  // used for ranges
  private byte[][] keys;
  private long[] counts;
  // used for set
  private TreeMap<byte[], Long> setMap;
  public static final String LC_TABLE_DESC_RANGE_DELIMITER = ",,";

  public LCStatInfo2(byte[] family, byte[] qualifier, DataType type, String[] allParts,
      int startIndex) throws IOException {
    this.family = family;
    this.qualifier = qualifier;
    this.type = type;
    this.isSet = true;
    this.setMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for (int i = startIndex; i < allParts.length; ++i) {
      setMap.put(DataType.stringToBytes(type, allParts[i]), (long) 0);
    }
  }

  public LCStatInfo2(byte[] family, byte[] qualifier, DataType type, int parts, String min,
      String max) throws IOException {
    this.family = family;
    this.qualifier = qualifier;
    this.type = type;
    this.isSet = false;
    switch (type) {
    case INT:
      parseInt(parts, min, max);
      break;
    case LONG:
      parseLong(parts, min, max);
      break;
    case DOUBLE:
      parseDouble(parts, min, max);
      break;
    default:
      throw new IOException("LCDBG, StatInfo ranges not support type: " + type);
    }
  }

  /**
   * create map based on statDesc
   * 1. for set, "family qualifier DataType set [v1] [v2] [...]"
   * 2. for array, "family qualifier DataType min max parts"
   */
  public static Map<TableName, LCStatInfo2> parseStatString(IndexTableRelation indexTableRelation,
      String statDesc) throws IOException {
    Map<TableName, LCStatInfo2> map = new HashMap<>();
    String[] lines = statDesc.split(LC_TABLE_DESC_RANGE_DELIMITER);
    for (String line : lines) {
      String[] parts = line.split("\t");
      byte[] family = Bytes.toBytes(parts[0]);
      byte[] qualifier = Bytes.toBytes(parts[1]);
      TableName tableName = indexTableRelation.getIndexTableName(family, qualifier);
      LCStatInfo2 statInfo;
      try {
        if ("set".equalsIgnoreCase(parts[3])) {
          statInfo = new LCStatInfo2(family, qualifier, DataType.valueOf(parts[2]), parts, 4);
        } else {
          statInfo = new LCStatInfo2(family, qualifier, DataType.valueOf(parts[2]),
              Integer.valueOf(parts[5]), parts[3], parts[4]);
        }
      } catch (IOException e) {
        throw new IOException("exception for parsing line: " + line, e);
      }
      map.put(tableName, statInfo);
    }
    return map;
  }

  private void initKV(int parts) {
    keys = new byte[parts + 1][];
    counts = new long[parts + 1];
  }

  private void parseInt(int parts, String minStr, String maxStr) {
    int min = Integer.valueOf(minStr);
    int max = Integer.valueOf(maxStr);
    if ((max - min) < parts) {
      parts = max - min;
      if (parts == 0) parts = 1;
    }
    initKV(parts);
    int interval = (max - min) / parts;
    int left = (max - min) % parts;
    for (int i = 0; i < parts; ++i) {
      int addition = i < left ? 1 : 0;
      keys[i] = Bytes.toBytes(min + i * interval + addition);
      counts[i] = 0;
    }
    keys[parts] = Bytes.toBytes(Integer.MAX_VALUE);
    counts[parts] = 0;
  }

  private void parseLong(int parts, String minStr, String maxStr) {
    long min = Long.valueOf(minStr);
    long max = Long.valueOf(maxStr);
    if ((max - min) < (long) parts) {
      parts = (int) (max - min);
    }
    initKV(parts);
    long interval = (max - min) / parts;
    long left = (max - min) % parts;
    for (int i = 0; i < parts; ++i) {
      int addition = i < left ? 1 : 0;
      keys[i] = Bytes.toBytes(min + i * interval + addition);
      counts[i] = 0;
    }
    keys[parts] = Bytes.toBytes(Long.MAX_VALUE);
    counts[parts] = 0;
  }

  private void parseDouble(int parts, String minStr, String maxStr) {
    double min = Double.valueOf(minStr);
    double max = Double.valueOf(maxStr);
    initKV(parts);
    double interval = (max - min) / parts;
    for (int i = 0; i < parts; ++i) {
      keys[i] = Bytes.toBytes(min + i * interval);
      counts[i] = 0;
    }
    keys[parts] = Bytes.toBytes(Double.MAX_VALUE);
    counts[parts] = 0;
  }

  @Override public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[").append(Bytes.toString(family)).append(":").append(Bytes.toString(qualifier))
        .append("]").append(",").append(type);
    if (isSet) {
      sb.append(", set values:");
      for (Map.Entry<byte[], Long> entry : setMap.entrySet()) {
        sb.append(LCIndexConstant.getStringOfValueAndType(type, entry.getKey())).append(":")
            .append(entry.getValue()).append("###");
      }
    } else {
      sb.append(", range values:");
      for (int i = 0; i < keys.length; ++i) {
        sb.append(LCIndexConstant.getStringOfValueAndType(type, keys[i])).append(":")
            .append(counts[i]).append("###");
      }
    }
    return sb.toString();
  }

  public void updateRange(byte[] value) throws IOException {
    if (isSet) {
      setMap.put(value, setMap.get(value) + 1);
    } else {
      updateRangeArray(value);
    }
  }

  private void updateRangeArray(byte[] value) {
    int index = binarySearch(value);
    ++counts[index];
  }

  public int binarySearch(byte[] value) {
    int low = 0;
    int high = keys.length - 1;
    int middle = 0;
    while (low <= high) {
      middle = low + ((high - low) >> 1);
      int ret = Bytes.compareTo(value, keys[middle]);
      if (ret == 0) {
        return middle;
      } else if (ret < 0) {
        high = middle - 1;
      } else {
        low = middle + 1;
      }
    }
    return middle;
  }

  public int getSetKeyIndex(byte[] targetKey) {
    if (!isSet) return -1;
    int counter = 0;
    for (byte[] key : setMap.keySet()) {
      if (Bytes.equals(key, targetKey)) {
        return counter;
      }
      ++counter;
    }
    return -2;
  }

  public byte[] getQualifier() {
    return qualifier;
  }

  public boolean isSet() {
    return isSet;
  }

  public Collection<Long> getSetValues() {
    return isSet ? setMap.values() : null;
  }

  public long[] getCounts() {
    return isSet ? null : counts;
  }

  public byte[][] getKeys() {
    return keys;
  }
}
