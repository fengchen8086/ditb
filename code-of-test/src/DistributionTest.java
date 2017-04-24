import ditb.ycsb.generator.CounterGenerator;
import ditb.ycsb.generator.ScrambledZipfianGenerator;
import ditb.ycsb.generator.SkewedLatestGenerator;
import ditb.ycsb.generator.UniformIntegerGenerator;
import ditb.ycsb.generator.ZipfianGenerator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.text.ParseException;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by winter on 16-10-24.
 */
public class DistributionTest {
  private static String adWorkloadClsName = "ditb.workload.AdWorkload";
  private static String adWorkloadDescPath =
      "/home/winter/workspace/git/LCIndex-HBase-1.2.1/scripts/ditb/conf/workload-ad";
  private static String uniWorkloadClsName = "ditb.workload.UniWorkload";
  private static String uniWorkloadDescPath =
      "/home/winter/workspace/git/LCIndex-HBase-1.2.1/scripts/ditb/conf/workload-uni";

  public static void main(String[] args) throws IOException, ParseException {
    new DistributionTest().work();
  }

  private void work() throws IOException, ParseException {
    useScrambledZipfianGenerator();
  }

  private void useUniformGenerator() {
    UniformIntegerGenerator gen = new UniformIntegerGenerator(1, 10);
    Map<Integer, Integer> map = new TreeMap<>();
    for (int i = 0; i < 100000; i++) {
      //      Long value = 100 - gen.nextValue();
      Integer value = gen.nextValue();
      if (map.containsKey(value)) {
        map.put(value, map.get(value) + 1);
      } else {
        map.put(value, 1);
      }
    }
    for (Map.Entry<Integer, Integer> entry : map.entrySet()) {
      System.out.println(entry.getKey() + "\t" + entry.getValue());
    }
  }

  private void useScrambledZipfianGenerator() {
    int min = 1, max = 13;
    ZipfianGenerator zipfGen = new ZipfianGenerator(min, max);
    ScrambledZipfianGenerator gen = new ScrambledZipfianGenerator(min, max, zipfGen);
    //    ScrambledZipfianGenerator gen = new ScrambledZipfianGenerator(min, max);
    Map<Long, Integer> map = new TreeMap<>();
    for (int i = 0; i < 100000; i++) {
      //      Long value = 100 - gen.nextValue();
      Long value = gen.nextValue();
      if (map.containsKey(value)) {
        map.put(value, map.get(value) + 1);
      } else {
        map.put(value, 1);
      }
    }
    for (Map.Entry<Long, Integer> entry : map.entrySet()) {
      System.out.println(entry.getKey() + "\t" + entry.getValue());
    }
  }

  private void useZipfianGenerator() {
    int min = 1, max = 10;
    ZipfianGenerator zipfGen = new ZipfianGenerator(min, max);
    Map<Long, Integer> map = new TreeMap<>();
    for (int i = 0; i < 100000; i++) {
      //      Long value = 100 - gen.nextValue();
      Long value = zipfGen.nextValue();
      if (map.containsKey(value)) {
        map.put(value, map.get(value) + 1);
      } else {
        map.put(value, 1);
      }
    }
    for (Map.Entry<Long, Integer> entry : map.entrySet()) {
      System.out.println(entry.getKey() + "\t" + entry.getValue());
    }
  }

  private void useSkewLatestGenerator() {
    CounterGenerator cg = new CounterGenerator(11);
    SkewedLatestGenerator gen = new SkewedLatestGenerator(cg);
    Map<Long, Integer> map = new TreeMap<>();
    for (int i = 0; i < 100000; i++) {
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
      System.out.println(entry.getKey() + "\t" + entry.getValue());
    }
  }

}
