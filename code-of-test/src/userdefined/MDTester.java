package userdefined;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.index.mdhbase.MDHBaseAdmin;
import org.apache.hadoop.hbase.index.mdhbase.MDPoint;
import org.apache.hadoop.hbase.index.mdhbase.MDRange;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by winter on 16-12-29.
 */
public class MDTester {
  TableName tableName = TableName.valueOf("tbl_md");
  String confFile =
      "/home/winter/workspace/git/LCIndex-HBase-1.2.1/scripts/local/winter-hbase.conf";
  int threshold = 10;

  public static void main(String[] args) throws IOException {
    new MDTester().largeTest();
  }

  private void largeTest() throws IOException {
    Random r = new Random();
    int MAX_NUMBER = 10000;
    Configuration conf = HBaseConfiguration.create();
    HRegionServer.loadWinterConf(conf, confFile);
    MDHBaseAdmin admin = new MDHBaseAdmin(conf, tableName, threshold, 3);
    int INSERT_NUMBER = 1000;
    int POINT_QUERY_NUMBER = 30;
    int SCAN_NUMBER = 10;
    int RANGE_CROSS = 10000;
    for (int i = 0; i < INSERT_NUMBER; i++) {
      int[] arr = new int[] { r.nextInt(MAX_NUMBER), r.nextInt(MAX_NUMBER), r.nextInt(MAX_NUMBER) };
      admin.insert(new MDPoint(Bytes.toBytes(i), arr));
    }
    for (int i = 0; i < POINT_QUERY_NUMBER; i++) {
      MDPoint p = new MDPoint(Bytes.toBytes(i),
          new int[] { r.nextInt(MAX_NUMBER), r.nextInt(MAX_NUMBER), r.nextInt(MAX_NUMBER) });
      StringBuilder sb = new StringBuilder();
      sb.append("get on ").append(p).append(" contains {");
      for (MDPoint pp : admin.get(p.values)) {
        sb.append(pp);
      }
      sb.append("}");
      System.out.println(sb.toString());
    }
    for (int i = 0; i < SCAN_NUMBER; i++) {
      int[] arr = new int[] { r.nextInt(MAX_NUMBER), r.nextInt(MAX_NUMBER), r.nextInt(MAX_NUMBER) };
      MDRange[] ranges = new MDRange[] { new MDRange(arr[0], arr[0] + RANGE_CROSS),
          new MDRange(arr[1], arr[1] + RANGE_CROSS), new MDRange(arr[0], arr[0] + RANGE_CROSS) };
      StringBuilder sb = new StringBuilder();
      sb.append("scan on {");
      for (MDRange range : ranges) {
        sb.append(range).append(",");
      }
      sb.append(" contains {");
      for (MDPoint pp : admin.rangeQuery(ranges, 100)) {
        sb.append(pp);
      }
      sb.append("} \n");
      System.out.println(sb.toString());
    }
  }

  private void basicTest() throws IOException {
    List<MDPoint> points = new ArrayList<>();
    int id = 1;
    points.add(new MDPoint(Bytes.toBytes(++id), new int[] { 1, 5, 5 }));
    points.add(new MDPoint(Bytes.toBytes(++id), new int[] { 5, 5, 3 }));
    points.add(new MDPoint(Bytes.toBytes(++id), new int[] { 3, 2, 5 }));
    points.add(new MDPoint(Bytes.toBytes(++id), new int[] { 10, 6, 7 }));
    points.add(new MDPoint(Bytes.toBytes(++id), new int[] { 6, 9, 4 }));
    points.add(new MDPoint(Bytes.toBytes(++id), new int[] { 4, 7, 4 }));
    points.add(new MDPoint(Bytes.toBytes(++id), new int[] { 2, 1, 5 }));
    List<MDRange[]> rangeList = new ArrayList<>();
    rangeList.add(new MDRange[] { new MDRange(1, 5), new MDRange(2, 5), new MDRange(3, 7) });
    rangeList.add(new MDRange[] { new MDRange(3, 4), new MDRange(6, 8), new MDRange(2, 5) });
    rangeList.add(new MDRange[] { new MDRange(2, 8), new MDRange(5, 10), new MDRange(1, 9) });
    mdWork(points, rangeList);
  }

  private void mdWork(List<MDPoint> points, List<MDRange[]> rangeList) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    HRegionServer.loadWinterConf(conf, confFile);
    StringBuilder sb = new StringBuilder();
    MDHBaseAdmin admin = new MDHBaseAdmin(conf, tableName, threshold, 3);
    for (MDPoint p : points) {
      admin.insert(p);
    }
    for (MDPoint p : points) {
      sb.append("get on ").append(p).append(" contains {");
      for (MDPoint pp : admin.get(p.values)) {
        sb.append(pp);
      }
      sb.append("}\n");
    }
    for (MDRange[] ranges : rangeList) {
      sb.append("scan on {");
      for (MDRange r : ranges) {
        sb.append(r).append(",");
      }
      sb.append(" contains {");
      for (MDPoint pp : admin.rangeQuery(ranges, 100)) {
        sb.append(pp);
      }
      sb.append("} \n");
    }
    System.out.println(sb.toString());
  }

}
