package ditb.put;

import ditb.util.DITBConstants;
import ditb.util.DITBUtil;
import ditb.workload.AbstractDITBRecord;
import ditb.workload.AbstractWorkload;
import org.apache.commons.io.FileUtils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Created by winter on 17-1-9.
 */
public class DITBDataGenerator {

  private final AbstractWorkload workload;
  private final List<String> clients;
  private final String outputDir;
  private final String statFilePath;
  private final int noProcess;
  private final int noThread;
  private final Random random = new Random();

  public DITBDataGenerator(String outputDir, String statName, String clientFile, int noProcess,
      int noThread, String workloadClassName, String workloadDescFile) throws IOException {
    this.outputDir = outputDir;
    this.noProcess = noProcess;
    this.noThread = noThread;
    clients = DITBUtil.getClientHosts(clientFile);
    // statFile = $outputDir/statName
    statFilePath = DITBUtil.getChildPath(outputDir, statName);
    // clear all $output/clientHost
    for (String client : clients) {
      String clientDir = DITBUtil.getChildPath(outputDir, client);
      File dir = new File(clientDir);
      if (dir.exists()) FileUtils.deleteDirectory(dir);
      dir.mkdirs();
    }
    workload = AbstractWorkload.getWorkload(workloadClassName, workloadDescFile);
  }

  public void work() throws IOException, ParseException {
    if (workload.getDataSource() == AbstractWorkload.DataSource.FROM_DISK) {
      double selectRate = workload.getSelectRate();
      List<File> files = workload.getSourceDataPath();
      System.out.println(
          "generating data files from " + files.size() + " files for " + clients.size()
              + " clients, each has " + noProcess + " processes with " + noThread
              + " threads, select rate is " + selectRate + ", stat written into: " + statFilePath
              + " workload=" + workload);
      loadFilesToClients(selectRate, files);
    } else if (workload.getDataSource() == AbstractWorkload.DataSource.RANDOM_GENE) {
      // generate directly to target file
      int totalOperations = workload.getTotalOperations();
      System.out.println(
          "generating data files for " + clients.size() + " clients, each has " + noProcess
              + " processes with " + noThread + " threads, totally " + totalOperations
              + " operations, stat written into: " + statFilePath + " workload=" + workload);
      int totalWritten = geneRecordsToFile(totalOperations);
      System.out.println("totally " + totalWritten + " operations, expected " + totalOperations);
    } else {
      throw new RuntimeException("not implemented yet");
    }
    // read files and write to target files
    // write stat
    countLinesAndWriteStat(statFilePath);
  }

  // load data from files, distribute lines to output files
  private void loadFilesToClients(double selectRate, List<File> files)
      throws IOException, ParseException {
    Map<String, BufferedWriter> writerMap = new HashMap<>();
    for (File file : files) {
      BufferedReader br = new BufferedReader(new FileReader(file));
      String line;
      while ((line = br.readLine()) != null) {
        if (random.nextDouble() >= selectRate) continue;
        AbstractDITBRecord record = workload.loadDITBRecord(line);
        writeToFile(record, line, outputDir, clients, noProcess, noThread, writerMap);
      }
      br.close();
    }
    for (BufferedWriter bw : writerMap.values()) {
      bw.close();
    }
  }

  private void writeToFile(AbstractDITBRecord record, String line, String outputDir,
      List<String> clients, int noProcess, int noThread, Map<String, BufferedWriter> writerMap)
      throws IOException {
    String clientDir =
        DITBUtil.getChildPath(outputDir, clients.get(random.nextInt(clients.size())));
    int processId = random.nextInt(noProcess);
    int threadId = random.nextInt(noThread);
    String fileName = DITBUtil.getDataFileName(clientDir, processId, threadId);
    BufferedWriter bw = writerMap.get(fileName);
    if (bw == null) {
      bw = new BufferedWriter(new FileWriter(fileName));
      writerMap.put(fileName, bw);
    }
    bw.write(line + "\n");
  }

  // generate records one by one
  private int geneRecordsToFile(int totalOperations) throws IOException {
    int eachInterval = totalOperations / clients.size() / noProcess / noThread;
    for (int clientIndex = 0; clientIndex < clients.size(); clientIndex++) {
      String clientDir = DITBUtil.getChildPath(outputDir, clients.get(clientIndex));
      for (int processIndex = 0; processIndex < noProcess; processIndex++) {
        for (int threadIndex = 0; threadIndex < noThread; threadIndex++) {
          String fileName = DITBUtil.getDataFileName(clientDir, processIndex, threadIndex);
          BufferedWriter bw = new BufferedWriter(new FileWriter(fileName));
          for (int k = 0; k < eachInterval; k++) {
            AbstractDITBRecord record = workload.geneDITBRecord(random);
            bw.write(record.toLine() + "\n");
          }
          bw.close();
        }
      }
    }
    return eachInterval * clients.size() * noProcess * noThread;
  }

  private void countLinesAndWriteStat(String statFileName) throws IOException {
    BufferedWriter bw = new BufferedWriter(new FileWriter(statFileName));
    for (String str : workload.statDescriptions()) {
      System.out.println("stat info: " + str);
      bw.write(str + "\n");
    }
    bw.close();
  }

  public static void usage() {
    System.out.println(
        "DITBDataGenerator outputDir statName clientFile noProcess noThread workloadClassName workloadDescFile");
  }

  public static void main(String[] args) throws NumberFormatException, IOException, ParseException {
    if (args.length < 7) {
      usage();
      System.out.println("current:");
      for (String str : args) {
        System.out.println(str);
      }
      return;
    }
    new DITBDataGenerator(args[0], args[1], args[2], Integer.valueOf(args[3]),
        Integer.valueOf(args[4]), args[5], args[6]).work();
  }

}
