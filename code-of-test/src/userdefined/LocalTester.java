package userdefined;

import org.apache.hadoop.hbase.index.IndexType;

import java.io.IOException;

public class LocalTester {

  public static void usage() {
    System.out.println("usage: java -cp CLASSPATH " + LocalTester.class.getName()
        + " indexType tableDescFilePath recordNumber(int) printResult(boolean)");
  }

  public static void main(String args[]) throws IOException, ClassNotFoundException {
    String[] testArgs = new String[] { "IRIndex",
        "/home/winter/workspace/git/LCIndex-HBase-1.2.1/scripts/local/winter-hbase.conf",
        "/home/winter/workspace/git/LCIndex-HBase-1.2.1/scripts/lingcloud/conf/table-desc", "1000",
        "false", "/home/winter/workspace/git/LCIndex-HBase-1.2.1/scripts/local/test-output" };
    if (args == null || args.length == 0) {
      args = testArgs;
    }
    if (args.length < testArgs.length) {
      usage();
      return;
    }
    System.out.println("running version 0.06");
    new LocalTester().work(IndexType.valueOf(args[0]), args[1], args[2], Integer.valueOf(args[3]),
        Boolean.valueOf(args[4]), args[5]);
  }

  public void work(IndexType indexType, String additionalConf, String tableDescFile,
      int recordNumber, boolean printResult, String outputFile) throws IOException {
    BaseRunner runner = new BaseRunner(indexType, tableDescFile);
    runner.init(additionalConf);
    runner.insertData(recordNumber);
    runner.queryData(printResult, outputFile);
    runner.report();
    runner.close();
  }
}
