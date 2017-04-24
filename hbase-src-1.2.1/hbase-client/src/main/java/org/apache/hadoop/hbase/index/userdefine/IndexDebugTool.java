package org.apache.hadoop.hbase.index.userdefine;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;
import java.util.Map;

/**
 * Created by winter on 16-11-24.
 */
public class IndexDebugTool {
  public static void printColumn(String prefix, byte[] f, byte[] q, String suffix) {
    System.out.println(prefix + " [" + Bytes.toString(f) + ":" + Bytes.toString(q) + "] " + suffix);
  }

  public static void printColumn(String prefix, byte[] f, byte[] q) {
    printColumn(prefix, f, q, "");
  }

  public static void printColumn(byte[] f, byte[] q, String suffix) {
    printColumn("", f, q, suffix);
  }

  public static void printPut(Put put) {
    StringBuilder sb = new StringBuilder();
    sb.append("{").append(Bytes.toStringBinary(put.getRow()));
    for (Map.Entry<byte[], List<Cell>> entry : put.getFamilyCellMap().entrySet()) {
      byte[] family = entry.getKey();
      for (Cell cell : entry.getValue()) {
        sb.append("[").append(Bytes.toString(family)).append(":")
            .append(Bytes.toString(CellUtil.cloneQualifier(cell))).append("]=")
            .append(Bytes.toStringBinary(CellUtil.cloneValue(cell)));
      }
    }
    System.out.println(sb.toString());
  }
}
