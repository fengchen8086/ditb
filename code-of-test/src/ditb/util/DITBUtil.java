package ditb.util;

import org.apache.hadoop.hbase.index.mdhbase.MDRange;
import org.apache.hadoop.hbase.index.scanner.ScanRange;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by winter on 17-3-7.
 */
public class DITBUtil {

  public static String toClientProcessId(String host, int processId) {
    return host + "-" + processId;
  }

  public static String getClientName(String clientProcessId) {
    return clientProcessId.split("-")[0];
  }

  public static int getProcessId(String clientProcessId) {
    return Integer.valueOf(clientProcessId.split("-")[1]);
  }

  public static final String FILE_NAME_FORMAT = "%03d-%03d";

  public static String getDataFileName(String baseDir, int processId, int threadId) {
    return baseDir + "/" + String.format(FILE_NAME_FORMAT, processId, threadId);
  }

  public static int readFromFile(String fileName, String parameterName, int defaultValue) {
    int ret = defaultValue;
    String wantedPrefix = "#" + parameterName;
    BufferedReader br = null;
    try {
      File file = new File(fileName);
      if (!file.exists() || !file.isFile()) {
        System.out.println("file " + fileName + " not exist or not a file");
      } else {
        br = new BufferedReader(new FileReader(file));
        String line;
        while ((line = br.readLine()) != null) {
          if (line.startsWith(wantedPrefix)) {
            ret = Integer.valueOf(line.split("=")[1]);
          }
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (br != null) {
        try {
          br.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
    System.out.println(parameterName + " = " + ret);
    return ret;
  }

  /**
   * @param clientFile filename which indicating client hostnames
   * @return hostnames
   * @throws IOException
   */
  public static List<String> getClientHosts(String clientFile) throws IOException {
    BufferedReader br = new BufferedReader(new FileReader(clientFile));
    List<String> clients = new ArrayList<>();
    String line;
    while ((line = br.readLine()) != null) {
      if (!line.startsWith("#")) {
        clients.add(line);
      }
    }
    br.close();
    return clients;
  }

  public static String getChildPath(String parent, String child) {
    return parent + "/" + child;
  }

  public static MDRange getMDRange(ScanRange range) {
    int min = nullOrEmpty(range.getStart()) ? 0 : Bytes.toInt(range.getStart());
    int max = nullOrEmpty(range.getStop()) ? Integer.MAX_VALUE : Bytes.toInt(range.getStop());
    return new MDRange(min, max);
  }

  private static boolean nullOrEmpty(byte[] bytes) {
    return bytes == null || bytes.length == 0;
  }
}
