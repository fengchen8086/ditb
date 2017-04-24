package ditb.perf;

import ditb.util.DITBUtil;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by winter on 17-3-8.
 */
public class PerfDataGenerator {

  public static final String DATA_FILE_NAME = "data";
  public static final String QUERY_FILE_NAME = "query";
  public static final String SCAN_FILE_NAME = "scan";
  private Random random = new Random();

  // init constants
  private String dataFilePath;
  private String queryFilePath;
  private String scanFilePath;
  private String dataSourceDir;

  private int nbRecords;
  private int nbGet;
  private int nbScan;

  // used to generate query file
  private final int REORDER_LIST_SIZE = 100000;

  public PerfDataGenerator(String dataSourceDir, int nbRecords, int nbGet, int nbScan)
      throws IOException {
    // simple parameters
    this.dataSourceDir = dataSourceDir;
    this.nbRecords = nbRecords;
    this.nbGet = nbGet;
    this.nbScan = nbScan;
    if (nbRecords < nbGet || nbRecords < nbScan) {
      throw new IOException("keep nbRecords the largest");
    }
    dataFilePath = DITBUtil.getChildPath(dataSourceDir, DATA_FILE_NAME);
    queryFilePath = DITBUtil.getChildPath(dataSourceDir, QUERY_FILE_NAME);
    scanFilePath = DITBUtil.getChildPath(dataSourceDir, SCAN_FILE_NAME);
    // complex parameters
  }

  // generate datafile, query file, and scan file
  private void work() throws IOException {
    System.out.println(
        "generating data files, query written into: " + queryFilePath + ", src=" + dataSourceDir);
    List<File> dataFiles = getDataFiles();
    loadFiles(dataFiles);
    reOrderFile(dataFilePath, queryFilePath, nbGet);
    reOrderFile(queryFilePath, scanFilePath, nbScan);
  }

  private List<File> getDataFiles() {
    ArrayList<File> list = new ArrayList<>();
    for (File dir : new File(dataSourceDir).listFiles()) {
      if (dir.isDirectory()) {
        for (File file : dir.listFiles()) {
          if (file.isFile()) list.add(file);
        }
      }
    }
    return list;
  }

  // load data from files, distribute lines to output files
  private void loadFiles(List<File> files) throws IOException {
    BufferedWriter bw = new BufferedWriter(new FileWriter(dataFilePath));
    int lineCounter = 0;
    for (File file : files) {
      BufferedReader br = new BufferedReader(new FileReader(file));
      String line;
      while ((line = br.readLine()) != null) {
        bw.write(line + "\n");
        if (++lineCounter >= nbRecords) break;
      }
      br.close();
      if (lineCounter >= nbRecords) break;
    }
    System.out.println("loaded " + lineCounter + " records load files, target " + nbRecords);
    bw.close();
  }

  // load data and reorder lines to another file
  private void reOrderFile(String sourceFile, String destFile, int targetSize) throws IOException {
    BufferedReader br = new BufferedReader(new FileReader(sourceFile));
    BufferedWriter bw = new BufferedWriter(new FileWriter(destFile));
    int doneRecords = 0;
    String line;
    ArrayList<String> srcLines = new ArrayList<>(REORDER_LIST_SIZE);
    while ((line = br.readLine()) != null) {
      if (srcLines.size() < REORDER_LIST_SIZE) {
        srcLines.add(line);
      } else {
        int index = random.nextInt(REORDER_LIST_SIZE);
        String target = srcLines.get(index);
        bw.write(target + "\n");
        srcLines.set(index, line);
        ++doneRecords;
      }
      if (doneRecords >= targetSize) break;
    }
    while (doneRecords < targetSize && !srcLines.isEmpty()) {
      int index = random.nextInt(srcLines.size());
      bw.write(srcLines.remove(index) + "\n");
      ++doneRecords;
    }
    br.close();
    bw.close();
  }

  public static void usage() {
    System.out.println("srcDir, nbRecords, nbRandomGet, nbScan");
  }

  public static void main(String[] args) throws IOException, ParseException {
    if (args.length < 4) {
      StringBuilder sb = new StringBuilder();
      sb.append("current parameters: ");
      for (String one : args) {
        sb.append(one).append(",");
      }
      usage();
      return;
    }
    new PerfDataGenerator(args[0], Integer.valueOf(args[1]), Integer.valueOf(args[2]),
        Integer.valueOf(args[3])).work();
  }
}
