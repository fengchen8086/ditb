import ditb.workload.AbstractDITBRecord;
import ditb.workload.AbstractWorkload;
import ditb.workload.TPCHWorkload;
import ditb.ycsb.generator.CounterGenerator;
import ditb.ycsb.generator.ScrambledZipfianGenerator;
import ditb.ycsb.generator.SkewedLatestGenerator;
import ditb.ycsb.generator.UniformIntegerGenerator;
import ditb.ycsb.generator.ZipfianGenerator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.index.IndexType;
import org.apache.hadoop.hbase.index.mdhbase.MDHBaseAdmin;
import org.apache.hadoop.hbase.index.mdhbase.MDUtils;
import org.apache.hadoop.hbase.index.userdefine.ColumnInfo;
import org.apache.hadoop.hbase.index.userdefine.IndexPutParser;
import org.apache.hadoop.hbase.index.userdefine.IndexTableRelation;
import org.apache.hadoop.hbase.regionserver.index.lmdindex.LMDBucket;
import org.apache.hadoop.hbase.regionserver.index.lmdindex.LMDIndexWriter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.Buffer;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

/**
 * Created by winter on 16-10-24.
 */
public class Hi {
  private static final Log LOG = LogFactory.getLog(Hi.class);

  private static String adWorkloadClsName = "ditb.workload.AdWorkload";
  private static String adWorkloadDescPath =
      "/home/winter/workspace/git/LCIndex-HBase-1.2.1/scripts/ditb/conf/workload-ad";
  private static String uniWorkloadClsName = "ditb.workload.UniWorkload";
  private static String uniWorkloadDescPath =
      "/home/winter/workspace/git/LCIndex-HBase-1.2.1/scripts/ditb/conf/workload-uni";

  public static void main(String[] args) throws IOException, ParseException {
    new Hi().work();
  }

  private void work() throws IOException, ParseException {
    String fileName =
        "/home/winter/workspace/git/LCIndex-HBase-1.2.1/gnuplot/data/estimate-MDIndex.dat";
    BufferedReader br = new BufferedReader(new FileReader(fileName));
    String line;
    boolean firstLine = true;
    double total = 0;
    while ((line = br.readLine()) != null) {
      if (firstLine) {
        firstLine = false;
        continue;
      }
      String[] splits = line.split("\t");
      double d = Math.abs(Double.valueOf(splits[1]) / Double.valueOf(splits[4]) - 1);
      System.out.println(String.format("error of %s is %.5f", splits[0], d));
      total += d;
    }
    System.out.println(String.format("avg error is %.5f", total / 10));
    br.close();
  }

  private void useUniformGenerator() {
    UniformIntegerGenerator gen = new UniformIntegerGenerator(0, 10);
    Map<Integer, Integer> map = new TreeMap<>();
    for (int i = 0; i < 10000; i++) {
      //      Long value = 100 - gen.nextValue();
      Integer value = gen.nextValue();
      if (map.containsKey(value)) {
        map.put(value, map.get(value) + 1);
      } else {
        map.put(value, 1);
      }
    }
    for (Map.Entry<Integer, Integer> entry : map.entrySet()) {
      System.out.println(entry.getKey() + " --> " + entry.getValue());
    }
  }

  private void useScrambledZipfianGenerator() {
    int min = 100, max = 200;
    ZipfianGenerator zipfGen = new ZipfianGenerator(min, max);
    //    ScrambledZipfianGenerator gen = new ScrambledZipfianGenerator(min, max, zipfGen);
    ScrambledZipfianGenerator gen = new ScrambledZipfianGenerator(min, max);
    Map<Long, Integer> map = new TreeMap<>();
    for (int i = 0; i < 10000; i++) {
      //      Long value = 100 - gen.nextValue();
      Long value = gen.nextValue();
      if (map.containsKey(value)) {
        map.put(value, map.get(value) + 1);
      } else {
        map.put(value, 1);
      }
    }
    for (Map.Entry<Long, Integer> entry : map.entrySet()) {
      System.out.println(entry.getKey() + " --> " + entry.getValue());
    }
  }

  private void useZipfianGenerator() {
    int min = 100, max = 200;
    ZipfianGenerator zipfGen = new ZipfianGenerator(min, max);
    Map<Long, Integer> map = new TreeMap<>();
    for (int i = 0; i < 10000; i++) {
      //      Long value = 100 - gen.nextValue();
      Long value = zipfGen.nextValue();
      if (map.containsKey(value)) {
        map.put(value, map.get(value) + 1);
      } else {
        map.put(value, 1);
      }
    }
    for (Map.Entry<Long, Integer> entry : map.entrySet()) {
      System.out.println(entry.getKey() + " --> " + entry.getValue());
    }
  }

  private void useSkewLatestGenerator() {
    CounterGenerator cg = new CounterGenerator(100);
    SkewedLatestGenerator gen = new SkewedLatestGenerator(cg);
    Map<Long, Integer> map = new TreeMap<>();
    for (int i = 0; i < 10000; i++) {
      //      cg.nextValue();
      //      Long value = 100 - gen.nextValue();
      Long value = gen.nextValue();
      if (map.containsKey(value)) {
        map.put(value, map.get(value) + 1);
      } else {
        map.put(value, 1);
      }
    }
    for (Map.Entry<Long, Integer> entry : map.entrySet()) {
      System.out.println(entry.getKey() + " --> " + entry.getValue());
    }
  }

}
