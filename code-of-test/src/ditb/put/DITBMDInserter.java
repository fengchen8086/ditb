package ditb.put;

import com.google.common.io.Closeables;
import ditb.workload.AbstractDITBRecord;
import ditb.workload.AbstractWorkload;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.index.IndexType;
import org.apache.hadoop.hbase.index.mdhbase.MDHBaseAdmin;
import org.apache.hadoop.hbase.index.mdhbase.MDPoint;
import org.apache.hadoop.hbase.index.userdefine.IndexTableRelation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by winter on 17-1-10.
 */
public class DITBMDInserter extends DITBInserterBase {

  private MDHBaseAdmin mdAdmin;
  private Admin admin;
  private Table rawTable;
  private Connection conn;

  private long insertRawTotalTime = 0;
  private long insertIndexTotalTime = 0;
  private long nbPuts = 0;

  public DITBMDInserter(Configuration conf, TableName tableName, String loadDataDir, int processId,
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
    mdAdmin.initTables(true);
  }

  @Override public void close() throws IOException {
    Closeables.closeQuietly(rawTable);
    Closeables.closeQuietly(mdAdmin);
    Closeables.closeQuietly(admin);
    Closeables.closeQuietly(conn);
    System.out.println(String.format(
        "MDIndex summary, %d puts, %.2f seconds for raw table (avg lat=%.7f), %.2f seconds for index table (avg lat=%.7f)",
        nbPuts, insertRawTotalTime / 1000.0, insertRawTotalTime / 1000.0 / nbPuts,
        insertIndexTotalTime / 1000.0, insertIndexTotalTime / 1000.0 / nbPuts));
  }

  @Override protected RunnableDataInserter getProperDataInserter(int id, int reportInterval,
      ConcurrentLinkedQueue<AbstractDITBRecord> queue, FinishCounter fc) throws IOException {
    rawTable = conn.getTable(tableName);
    return new MDDataInserter(id, reportInterval, queue, fc, mdAdmin, rawTable);
  }

  public class MDDataInserter extends RunnableDataInserter {

    MDHBaseAdmin mdAdmin;
    Table rawTable;

    public MDDataInserter(int id, int reportInterval,
        ConcurrentLinkedQueue<AbstractDITBRecord> queue, FinishCounter fc, MDHBaseAdmin mdAdmin,
        Table rawTable) {
      super(id, reportInterval, queue, fc);
      this.mdAdmin = mdAdmin;
      this.rawTable = rawTable;
    }

    @Override protected void insertOneRecord(AbstractDITBRecord record) throws IOException {
      long startTime = System.currentTimeMillis();
      rawTable.put(record.getPut());
      insertRawTotalTime += System.currentTimeMillis() - startTime;
      startTime = System.currentTimeMillis();
      mdAdmin.insert(record.toMDPoint());
      insertIndexTotalTime += System.currentTimeMillis() - startTime;
      ++nbPuts;
    }

    @Override protected void insertRecords(List<AbstractDITBRecord> records) throws IOException {
      List<Put> puts = new ArrayList<>();
      List<MDPoint> points = new ArrayList<>();
      for (AbstractDITBRecord record : records) {
        puts.add(record.getPut());
        points.add(record.toMDPoint());
      }
      long startTime = System.currentTimeMillis();
      rawTable.put(puts);
      insertRawTotalTime += System.currentTimeMillis() - startTime;
      startTime = System.currentTimeMillis();
      mdAdmin.insert(points);
      insertIndexTotalTime += System.currentTimeMillis() - startTime;
      nbPuts += records.size();
    }
  }
}
