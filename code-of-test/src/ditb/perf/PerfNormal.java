package ditb.perf;

import ditb.workload.AbstractDITBRecord;
import ditb.workload.AbstractWorkload;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.index.IndexType;
import org.apache.hadoop.hbase.index.userdefine.IndexPutParser;
import org.apache.hadoop.hbase.index.userdefine.IndexTableAdmin;
import org.apache.hadoop.hbase.index.userdefine.IndexTableRelation;

import java.io.IOException;
import java.text.ParseException;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by winter on 17-3-11.
 */
public class PerfNormal {

  private IndexTableRelation relation;
  private TableName opTableName;
  private TableName givenTableName;
  private IndexType givenIndexType;
  private IndexType tableIndexType;
  private IndexPutParser putParser;
  AbstractWorkload workload;

  public PerfNormal(TableName tableName, IndexType indexType, AbstractWorkload workload)
      throws IOException {
    this.givenTableName = tableName;
    this.givenIndexType = indexType;
    this.workload = workload;
    // PERF_CCT, PERF_CCIT, PERF_Secondary, NoIndex
    if (indexType == IndexType.NoIndex) {
      tableIndexType = IndexType.NoIndex;
    } else if (indexType == IndexType.PF_CCT_ || indexType == IndexType.PF_CCIT) {
      tableIndexType = IndexType.CCIndex;
    } else if (indexType == IndexType.PF_SCND) {
      tableIndexType = IndexType.GSIndex;
    }
    relation = workload.getTableRelation(tableName, tableIndexType);
    putParser = IndexPutParser.getParser(tableIndexType, relation);
    if (indexType == IndexType.NoIndex) {
      opTableName = givenTableName;
    } else {
      for (Map.Entry<byte[], TreeSet<byte[]>> entry : relation.getIndexFamilyMap().entrySet()) {
        for (byte[] qualifier : entry.getValue()) {
          if (indexType == IndexType.PF_CCT_) {
            opTableName = relation.getCCTName(entry.getKey(), qualifier);
          } else {
            opTableName = relation.getIndexTableName(entry.getKey(), qualifier);
          }
          break;
        }
      }
    }
    System.out.println("PerfNormal " + indexType + " on table " + opTableName);
  }

  public PerfInserterBase getPerfInserter(String loadDataDir, int processId, int threadNum,
      String statFilePath, ConcurrentLinkedQueue<String> reportQueue) throws IOException {
    return new PerfNormalInserter(loadDataDir, processId, threadNum, statFilePath, reportQueue,
        workload);
  }

  public PerfScanBase getPerfScan(String dataDstDir, int nbGet, int nbScan, int sizeScanCovering,
      int testTimes) throws IOException {
    if (tableIndexType == IndexType.NoIndex) {
      return new PerfNoIndexScan(givenIndexType, workload, dataDstDir, nbGet, nbScan,
          sizeScanCovering, testTimes);
    } else {
      return new PerfNormalScan(givenIndexType, workload, dataDstDir, nbGet, nbScan,
          sizeScanCovering, testTimes);
    }
  }

  public class PerfNormalInserter extends PerfInserterBase {

    private IndexTableAdmin indexTableAdmin;
    private Connection conn;

    public PerfNormalInserter(String loadDataDir, int processId, int threadNum, String statFilePath,
        ConcurrentLinkedQueue<String> reportQueue, AbstractWorkload workload) throws IOException {
      super(workload.getHBaseConfiguration(), givenTableName, loadDataDir, processId, threadNum,
          statFilePath, reportQueue, workload);
      conn = ConnectionFactory.createConnection(conf);
      indexTableAdmin = new IndexTableAdmin(conf, conn, relation);
      conn = ConnectionFactory.createConnection(conf);
    }

    @Override public void close() throws IOException {
      indexTableAdmin.close();
      conn.close();
    }

    @Override protected void checkTable(byte[][] splits) throws IOException {
      System.out.println("create table with " + (splits == null ? 0 : splits.length) + " regions");
      indexTableAdmin.createTable(true, true, splits);
    }

    @Override protected RunnablePerfInserter getProperDataInserter(int id, int reportInterval,
        ConcurrentLinkedQueue<AbstractDITBRecord> queue, FinishCounter fc) throws IOException {
      if (tableIndexType == IndexType.NoIndex) {
        return new NoIndexInserter(id, reportInterval, queue, fc);
      } else {
        return new NormalPerfRunner(id, reportInterval, queue, fc);
      }
    }

    private class NormalPerfRunner extends RunnablePerfInserter {
      Table table;

      public NormalPerfRunner(int id, int reportInterval,
          ConcurrentLinkedQueue<AbstractDITBRecord> queue, FinishCounter fc) throws IOException {
        super(id, reportInterval, queue, fc);
        table = conn.getTable(opTableName);
      }

      @Override protected void insertOneRecord(AbstractDITBRecord record) throws IOException {
        Put put = record.getPut();
        Put realPut = putParser.parsePut(put).get(opTableName);
        table.put(realPut);
      }
    }

    private class NoIndexInserter extends RunnablePerfInserter {
      Table table;

      public NoIndexInserter(int id, int reportInterval,
          ConcurrentLinkedQueue<AbstractDITBRecord> queue, FinishCounter fc) throws IOException {
        super(id, reportInterval, queue, fc);
        table = conn.getTable(opTableName);
      }

      @Override protected void insertOneRecord(AbstractDITBRecord record) throws IOException {
        Put put = record.getPut();
        table.put(put);
      }
    }
  }

  public class PerfNormalScan extends PerfScanBase {

    public PerfNormalScan(IndexType indexType, AbstractWorkload workload, String dataDstDir,
        int nbGet, int nbScan, int sizeScanCovering, int testTimes) throws IOException {
      super(indexType, workload, dataDstDir, nbGet, nbScan, sizeScanCovering, testTimes);
    }

    @Override protected TableName getOpTableName() {
      return opTableName;
    }

    @Override protected int recordsInOneResult(Result result) {
      return 1;
    }

    @Override protected Get getIndexTableGet(String line) throws IOException, ParseException {
      AbstractDITBRecord record = workload.loadDITBRecord(line);
      Put rawPut = record.getPut();
      byte[] row = putParser.parsePut(rawPut).get(opTblName).getRow();
      return new Get(row);
    }

    @Override protected boolean hasRandomGet() {
      return false;
    }

    @Override protected boolean hasSequenceGet() {
      return false;
    }

    @Override protected boolean hasScan() {
      if (indexType == IndexType.PF_CCT_) return false;
      return true;
    }

    @Override protected byte[] getIndexTableScanStartKey(String line)
        throws IOException, ParseException {
      AbstractDITBRecord record = workload.loadDITBRecord(line);
      Put rawPut = record.getPut();
      return putParser.parsePut(rawPut).get(opTblName).getRow();
    }
  }

  public class PerfNoIndexScan extends PerfScanBase {

    public PerfNoIndexScan(IndexType indexType, AbstractWorkload workload, String dataDstDir,
        int nbGet, int nbScan, int sizeScanCovering, int testTimes) throws IOException {
      super(indexType, workload, dataDstDir, nbGet, nbScan, sizeScanCovering, testTimes);
    }

    @Override protected TableName getOpTableName() {
      return opTableName;
    }

    @Override protected int recordsInOneResult(Result result) {
      return 1;
    }

    @Override protected boolean hasRandomGet() {
      return true;
    }

    @Override protected boolean hasSequenceGet() {
      return true;
    }

    @Override protected Get getIndexTableGet(String line) throws IOException, ParseException {
      AbstractDITBRecord record = workload.loadDITBRecord(line);
      return new Get(record.getRowkey());
    }

    @Override protected boolean hasScan() {
      return true;
    }

    @Override protected byte[] getIndexTableScanStartKey(String line)
        throws IOException, ParseException {
      AbstractDITBRecord record = workload.loadDITBRecord(line);
      return record.getRowkey();
    }
  }
}
