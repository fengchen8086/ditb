package ditb.hybrid;

import com.google.common.io.Closeables;
import ditb.put.DITBInserterBase;
import ditb.workload.AbstractDITBRecord;
import ditb.workload.AbstractWorkload;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.index.IndexType;
import org.apache.hadoop.hbase.index.mdhbase.MDHBaseAdmin;
import org.apache.hadoop.hbase.index.mdhbase.MDPoint;
import org.apache.hadoop.hbase.index.mdhbase.MDRange;
import org.apache.hadoop.hbase.index.mdhbase.MDScanner;
import org.apache.hadoop.hbase.index.scanner.ScanRange;
import org.apache.hadoop.hbase.index.userdefine.IndexTableRelation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by winter on 17-1-10.
 */
public class HybridMDWorker extends HybridWorker {

  private MDHBaseAdmin mdAdmin;
  private Admin admin;
  private Table rawTable;
  private Connection conn;

  public HybridMDWorker(Configuration conf, TableName tableName, String loadDataDir, int processId,
      int threadNum, String statFilePath, ConcurrentLinkedQueue<String> reportQueue,
      IndexType indexType, AbstractWorkload workload) throws IOException {
    super(conf, tableName, loadDataDir, processId, threadNum, statFilePath, reportQueue, workload);
    conn = ConnectionFactory.createConnection(conf);
    admin = conn.getAdmin();
    mdAdmin = new MDHBaseAdmin(conf, tableName, workload.getMDBucketThreshold(),
        workload.getMDDimensions());
  }

  @Override protected void checkTable(byte[][] splits) throws IOException {
    System.out.println(
        "create table with " + (splits == null ? "default 1" : splits.length) + " regions");
    if (admin.tableExists(tableName)) {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(IndexTableRelation.getDefaultColumnDescriptor(workload.FAMILY_NAME));
    admin.createTable(desc, splits);
    mdAdmin.initTables(false);
  }

  @Override public void close() throws IOException {
    Closeables.closeQuietly(rawTable);
    Closeables.closeQuietly(mdAdmin);
    Closeables.closeQuietly(admin);
    Closeables.closeQuietly(conn);
  }

  @Override protected OperationExecutor getOperationExecutor(int id, int reportInterval,
      ConcurrentLinkedQueue<Operation> queue, FinishCounter fc) throws IOException {
    rawTable = conn.getTable(tableName);
    return new MDOperationExecutor(id, reportInterval, queue, fc, mdAdmin, rawTable);
  }

  public class MDOperationExecutor extends OperationExecutor {

    MDHBaseAdmin mdAdmin;
    Table rawTable;

    public MDOperationExecutor(int id, int reportInterval, ConcurrentLinkedQueue<Operation> queue,
        FinishCounter fc, MDHBaseAdmin mdAdmin, Table rawTable) {
      super(id, reportInterval, queue, fc);
      this.mdAdmin = mdAdmin;
      this.rawTable = rawTable;
    }

    @Override protected void executeWrite(Operation operation) throws IOException {
      rawTable.put(operation.getRecord().getPut());
      mdAdmin.insert(operation.getRecord().toMDPoint());
    }

    @Override protected void executeRead(Operation operation) throws IOException {
      rawTable.get(operation.getGet());
    }

    @Override protected int executeScan(Operation operation) throws IOException {
      ScanRange.ScanRangeList rangeList =
          ScanRange.ScanRangeList.getScanRangeList(operation.getScan());
      MDRange[] ranges = workload.getMDRanges(rangeList);
      ResultScanner scanner = new MDScanner(mdAdmin, rawTable, ranges, workload.getScanCacheSize());
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
