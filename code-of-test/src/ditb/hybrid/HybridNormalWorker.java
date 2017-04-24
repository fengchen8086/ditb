package ditb.hybrid;

import ditb.workload.AbstractWorkload;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.index.IndexType;
import org.apache.hadoop.hbase.index.scanner.BaseIndexScanner;
import org.apache.hadoop.hbase.index.userdefine.IndexTableAdmin;
import org.apache.hadoop.hbase.index.userdefine.IndexTableRelation;
import org.apache.hadoop.hbase.regionserver.index.lcindex.LCIndexConstant;
import org.apache.hadoop.hbase.regionserver.index.lcindex.LCStatInfo2;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by winter on 17-1-10.
 */
public class HybridNormalWorker extends HybridWorker {

  private IndexTableAdmin indexTableAdmin;
  private Connection conn;
  private IndexTableRelation relation;

  private IndexType indexType;
  private String statFilePath;

  public HybridNormalWorker(Configuration conf, TableName tableName, String loadDataDir,
      int processId, int threadNum, String statFilePath, ConcurrentLinkedQueue<String> reportQueue,
      IndexType indexType, AbstractWorkload workload) throws IOException {
    super(conf, tableName, loadDataDir, processId, threadNum, statFilePath, reportQueue, workload);
    conn = ConnectionFactory.createConnection(conf);
    relation = workload.getTableRelation(tableName, indexType);
    indexTableAdmin = new IndexTableAdmin(conf, conn, relation);
    this.indexType = indexType;
    this.statFilePath = statFilePath;
  }

  @Override public void close() throws IOException {
    indexTableAdmin.close();
    conn.close();
  }

  @Override protected void checkTable(byte[][] splits) throws IOException {
    if (indexType == IndexType.LCIndex) {
      indexTableAdmin.setLCIndexRange(getRangeString());
    }
    if (indexType == IndexType.LMDIndex_D || indexType == IndexType.LMDIndex_S) {
      indexTableAdmin.setLMDIndexThreshold(workload.getMDBucketThreshold());
    }
    System.out.println(
        "create table with " + (splits == null ? "default 1" : splits.length) + " regions");
    indexTableAdmin.createTable(false, false, splits);
  }

  public String getRangeString() throws IOException {
    StringBuilder sb = new StringBuilder();
    BufferedReader br = new BufferedReader(new FileReader(statFilePath));
    String line;
    while ((line = br.readLine()) != null) {
      if (line.equalsIgnoreCase(LCIndexConstant.ROWKEY_RANGE)) continue;
      sb.append(Bytes.toString(workload.FAMILY_NAME)).append("\t").append(line);
      sb.append(LCStatInfo2.LC_TABLE_DESC_RANGE_DELIMITER);
    }
    return sb.toString();
  }

  @Override protected OperationExecutor getOperationExecutor(int id, int reportInterval,
      ConcurrentLinkedQueue<Operation> queue, FinishCounter fc) throws IOException {
    return new NormalWoker(id, reportInterval, queue, fc, indexTableAdmin);
  }

  private class NormalWoker extends OperationExecutor {
    private IndexTableAdmin indexTableAdmin;
    private Random random = new Random();

    public NormalWoker(int id, int reportInterval, ConcurrentLinkedQueue<Operation> queue,
        FinishCounter fc, IndexTableAdmin indexTableAdmin) {
      super(id, reportInterval, queue, fc);
      this.indexTableAdmin = indexTableAdmin;
    }

    @Override protected void executeWrite(Operation operation) throws IOException {
      indexTableAdmin.putWithIndex(operation.getRecord().getPut());
    }

    @Override protected void executeRead(Operation operation) throws IOException {
      indexTableAdmin.getWithoutIndex(operation.getGet());
    }

    @Override protected int executeScan(Operation operation) throws IOException {
      Scan scan = operation.getScan();
      scan.setCaching(workload.getScanCacheSize());
      scan.setCacheBlocks(true);
      ResultScanner scanner = BaseIndexScanner.getIndexScanner(conn, relation, scan);
      int resultCount = 0;
      while (true) {
        Result[] results = scanner.next(workload.getScanCacheSize());
        if (results == null || results.length == 0) break;
        resultCount += results.length;
      }
      return resultCount;
    }
  }
}
