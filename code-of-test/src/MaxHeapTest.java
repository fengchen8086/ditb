import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

/**
 * Created by winter on 17-3-7.
 */
public class MaxHeapTest {

  private static void usage() {
    System.out.println("Type(Link or Array), MaxTestSize, PrintTimes, EachRecordSize(in int)");
  }

  public static void main(String[] args) throws IOException {
    args = new String[]{"array", "1000", "10", "5"};
    if (args.length < 4) {
      usage();
      return;
    }
    new MaxHeapTest().work(args[0], Integer.valueOf(args[1]), Integer.valueOf(args[2]),
        Integer.valueOf(args[3]));
  }

  private void work(String type, int expectSize, int printTimes, int numberOfInt) {
    System.out.println(String
        .format("using %s to test %d records with %d byte, print %d times", type, expectSize,
            numberOfInt * 4, printTimes));
    Random random = new Random();
    List<byte[]> list;
    if (type.equalsIgnoreCase("Link")) {
      list = new LinkedList<>();
    } else {
      list = new ArrayList<>();
    }
    int printInterval = expectSize / printTimes;
    int printCount = 0;
    for (int i = 0; i < expectSize; i++) {
      byte[] row = new byte[numberOfInt * 4];
      for (int j = 0; j < numberOfInt; j++) {
        byte[] tmp = Bytes.toBytes(random.nextInt());
        System.arraycopy(tmp, 0, row, j * 4, tmp.length);
      }
      list.add(row);
      printCount++;
      if (printCount == printInterval) {
        System.out.println(String.format("expect %d, now %d", expectSize, (i+1)));
        printCount = 0;
      }
    }
    System.out.println(String
        .format("success in using %s to test %d records with %d byte", type, expectSize,
            numberOfInt * 4));
  }
}
