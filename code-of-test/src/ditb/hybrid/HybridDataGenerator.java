package ditb.hybrid;

import ditb.util.DITBUtil;
import ditb.workload.AbstractDITBRecord;
import ditb.workload.AbstractWorkload;
import org.apache.commons.io.FileUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.Random;

/**
 * Created by winter on 17-1-9.
 */
public class HybridDataGenerator {

  private final AbstractWorkload workload;
  private final List<String> clients;
  private final String outputDir;
  private final String statFilePath;
  private final int nbProcess;
  private final int nbThread;
  private final Random random = new Random();
  OperationSelector operationSelector;

  public HybridDataGenerator(String outputDir, String statName, String clientFile, int noProcess,
      int nbThread, String workloadClassName, String workloadDescFile) throws IOException {
    this.outputDir = outputDir;
    this.nbProcess = noProcess;
    this.nbThread = nbThread;
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
    operationSelector = new OperationSelector(workloadDescFile);
  }

  public void work() throws IOException, ParseException {
    if (workload.getDataSource() == AbstractWorkload.DataSource.RANDOM_GENE) {
      // generate directly to target file
      int totalOperations = workload.getTotalOperations();
      int operationPerFile = totalOperations / clients.size() / nbProcess / nbThread;
      System.out.println(String.format(
          "generating data files for %d clients, each has %d processes with %d threads, "
              + " each has %d operations, stat written into %s, workload is %s", clients.size(),
          nbProcess, nbThread, operationPerFile, statFilePath, workload));
      if (operationSelector.hasWrite()) {
        geneComplexRecordsToFile(operationPerFile);
      } else { // pure read and scan
        geneReadAndScanOperationsToFile(operationPerFile, workload.getNbExistingRows());
      }
    } else {
      throw new RuntimeException("not implemented yet");
    }
    // read files and write to target files
    // write stat
    countLinesAndWriteStat(statFilePath);
  }

  // generate records one by one, has write, read and scan
  private void geneComplexRecordsToFile(int operationsPerFile) throws IOException {
    int written = 1;
    for (int clientIndex = 0; clientIndex < clients.size(); clientIndex++) {
      String clientDir = DITBUtil.getChildPath(outputDir, clients.get(clientIndex));
      for (int processIndex = 0; processIndex < nbProcess; processIndex++) {
        for (int threadIndex = 0; threadIndex < nbThread; threadIndex++) {
          int w = 0, r = 0, s = 0;
          String fileName = DITBUtil.getDataFileName(clientDir, processIndex, threadIndex);
          BufferedWriter bw = new BufferedWriter(new FileWriter(fileName));
          int minId = written;
          // give it a write at first
          String line = operationSelector.getWriteOperation().getOperationName() + workload
              .geneDITBRecord(written, random).toLine();
          bw.write(line + "\n");
          ++w;
          for (int k = 0; k < operationsPerFile; k++) {
            OperationSelector.OperationRatio operation = operationSelector.nextOperation();
            if (operation.isWrite()) {
              AbstractDITBRecord record = workload.geneDITBRecord(++written, random);
              line = operation.getOperationName() + record.toLine();
              ++w;
            } else if (operation.isRead()) {
              line =
                  operation.getOperationName() + ((OperationSelector.ReadOperationRatio) operation)
                      .getReadRowkey(minId, written);
              ++r;
            } else if (operation.isScan()) {
              line =
                  operation.getOperationName() + ((OperationSelector.ScanOperationRatio) operation)
                      .getScanFileName();
              ++s;
            } else {
              throw new RuntimeException("unknown operation: " + operation.getOperationName());
            }
            //            System.out.println("op line: " + line);
            bw.write(line + "\n");
          }
          System.out.println(String.format("in file %s, w=%d, r=%d, s=%d", fileName, w, r, s));
          bw.close();
        }
      }
    }
  }

  private void geneReadAndScanOperationsToFile(int operationsPerFile, int nbExistingRows)
      throws IOException {
    int minId = 0, maxId = nbExistingRows;
    for (int clientIndex = 0; clientIndex < clients.size(); clientIndex++) {
      String clientDir = DITBUtil.getChildPath(outputDir, clients.get(clientIndex));
      for (int processIndex = 0; processIndex < nbProcess; processIndex++) {
        for (int threadIndex = 0; threadIndex < nbThread; threadIndex++) {
          String fileName = DITBUtil.getDataFileName(clientDir, processIndex, threadIndex);
          BufferedWriter bw = new BufferedWriter(new FileWriter(fileName));
          for (int k = 0; k < operationsPerFile; k++) {
            OperationSelector.OperationRatio operation = operationSelector.nextOperation();
            String line;
            if (operation.isRead()) {
              line =
                  operation.getOperationName() + ((OperationSelector.ReadOperationRatio) operation)
                      .getReadRowkey(minId, maxId);
            } else if (operation.isScan()) {
              line =
                  operation.getOperationName() + ((OperationSelector.ScanOperationRatio) operation)
                      .getScanFileName();
            } else {
              throw new RuntimeException(
                  "unknown operation in read and scan only: " + operation.getOperationName());
            }
            //            System.out.println("op line: " + line);
            bw.write(line + "\n");
          }
          bw.close();
        }
      }
    }
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
        "HybridDataGenerator outputDir statName clientFile nbProcess nbThread workloadClassName workloadDescFile");
  }

  public static void main(String[] args) throws NumberFormatException, IOException, ParseException {
    //    args = new String[] { "/home/winter/data/ditb-out", "stat.dat",
    //        "/home/winter/workspace/git/LCIndex-HBase-1.2.1/scripts/ditb/conf/clients", "3", "3",
    //        "ditb.workload.ExampleAWorkload",
    //        "/home/winter/workspace/git/LCIndex-HBase-1.2.1/scripts/ditb/conf/workload-a" };
    if (args.length < 7) {
      usage();
      System.out.println("current:");
      for (String str : args) {
        System.out.println(str);
      }
      return;
    }
    new HybridDataGenerator(args[0], args[1], args[2], Integer.valueOf(args[3]),
        Integer.valueOf(args[4]), args[5], args[6]).work();
  }
}
