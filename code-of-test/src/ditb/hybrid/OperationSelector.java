package ditb.hybrid;

import ditb.ycsb.generator.CounterGenerator;
import ditb.ycsb.generator.ScrambledZipfianGenerator;
import ditb.ycsb.generator.SkewedLatestGenerator;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by winter on 17-3-20.
 */
public class OperationSelector {

  public abstract class OperationRatio {
    protected static final String SUB_STRING_DELIMITER = ",";
    protected static final String KEY_VALUE_DELIMITER = "=";
    protected String name;
    protected double portion;

    protected double getPortion(String keyValue) {
      String[] splits = keyValue.split(KEY_VALUE_DELIMITER);
      System.out.println(keyValue);
      return Double.valueOf(splits[1]);
    }

    public double getPortion() {
      return portion;
    }

    public String getOperationName() {
      return name;
    }

    public boolean isWrite() {
      return false;
    }

    public boolean isRead() {
      return false;
    }

    public boolean isScan() {
      return false;
    }
  }

  public class WriteOperationRatio extends OperationRatio {
    public WriteOperationRatio(String[] subStrings) {
      name = Operation.WriteOperation.OperationPrefix;
      assert subStrings.length == 1;
      portion = getPortion(subStrings[0]);
    }

    @Override public boolean isWrite() {
      return true;
    }
  }

  public class ReadOperationRatio extends OperationRatio {
    protected String distribution;
    private long lastMinId = -1;
    private long lastMaxId = -1;
    ScrambledZipfianGenerator zipfianGenerator = null;
    SkewedLatestGenerator latestGenerator = null;

    public ReadOperationRatio(String[] subStrings) {
      name = Operation.ReadOperation.OperationPrefix;
      assert subStrings.length == 2;
      portion = getPortion(subStrings[0]);
      String[] splits = subStrings[1].split(KEY_VALUE_DELIMITER);
      assert splits[0].equals("distribution");
      distribution = splits[1];
    }

    @Override public boolean isRead() {
      return true;
    }

    public int getReadRowkey(int minId, int maxId) {
      if (distribution.equals("uniform")) {
        int items = maxId - minId + 1;
        return minId + random.nextInt(items);
      } else if (distribution.equals("latest")) {
        if (lastMaxId != maxId || lastMinId != minId || latestGenerator != null) {
          lastMaxId = maxId;
          lastMinId = minId;
          latestGenerator = new SkewedLatestGenerator(new CounterGenerator(maxId - minId + 1));
        }
        return (int) (latestGenerator.nextValue() + minId);
      } else if (distribution.equals("zipfian")) {
        if (lastMaxId != maxId || lastMinId != minId || zipfianGenerator != null) {
          lastMaxId = maxId;
          lastMinId = minId;
          zipfianGenerator = new ScrambledZipfianGenerator(minId, maxId);
        }
        return Math.toIntExact(zipfianGenerator.nextValue());
      }
      throw new RuntimeException("unknown distribution: " + distribution);
    }
  }

  public class ScanOperationRatio extends OperationRatio {

    String fileName;

    public ScanOperationRatio(String[] subStrings) {
      name = Operation.ScanOperation.OperationPrefix;
      assert subStrings.length == 2;
      portion = getPortion(subStrings[0]);
      String[] splits = subStrings[1].split(KEY_VALUE_DELIMITER);
      assert splits[0].equals("file");
      fileName = splits[1];
    }

    @Override public boolean isScan() {
      return true;
    }

    public String getScanFileName() {
      return fileName;
    }
  }

  private List<OperationRatio> operations = new ArrayList<>();
  private Random random = new Random();
  private boolean hasWrite = false;

  public OperationSelector(String fileName) throws IOException {
    BufferedReader br = new BufferedReader(new FileReader(fileName));
    String line;
    while ((line = br.readLine()) != null) {
      if (line.startsWith("#")) continue;
      OperationRatio operation = null;
      if (line.startsWith(Operation.WriteOperation.OperationPrefix)) {
        operation =
            new WriteOperationRatio(line.substring(9).split(OperationRatio.SUB_STRING_DELIMITER));
        if (operation.getPortion() > 0) hasWrite = true;
      } else if (line.startsWith(Operation.ReadOperation.OperationPrefix)) {
        operation =
            new ReadOperationRatio(line.substring(8).split(OperationRatio.SUB_STRING_DELIMITER));
      } else if (line.startsWith(Operation.ScanOperation.OperationPrefix)) {
        operation =
            new ScanOperationRatio(line.substring(8).split(OperationRatio.SUB_STRING_DELIMITER));
      }
      if (operation != null && operation.getPortion() > 0) operations.add(operation);
    }
    br.close();
  }

  public boolean hasWrite() {
    return hasWrite;
  }

  public OperationRatio nextOperation() {
    double total = 0;
    double value = random.nextDouble();
    for (OperationRatio operation : operations) {
      total += operation.getPortion();
      if (value <= total) {
        return operation;
      }
    }
    return operations.get(operations.size() - 1);
  }

  public OperationRatio getWriteOperation() {
    for(OperationRatio operationRatio: operations){
      if(operationRatio.isWrite())
        return operationRatio;
    }
    return null;
  }

  public static void main(String[] args) throws IOException {
    String fileName = "/home/winter/workspace/git/LCIndex-HBase-1.2.1/scripts/ditb/conf/workload-a";
    OperationSelector selector = new OperationSelector(fileName);
    for (int i = 0; i < 100; i++) {
      OperationRatio op = selector.nextOperation();
    }
  }

}
