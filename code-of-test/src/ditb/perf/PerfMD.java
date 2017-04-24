package ditb.perf;

import com.google.common.io.Closeables;
import ditb.workload.AbstractDITBRecord;
import ditb.workload.AbstractWorkload;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.index.IndexType;
import org.apache.hadoop.hbase.index.mdhbase.MDHBaseAdmin;
import org.apache.hadoop.hbase.index.mdhbase.MDIndex;
import org.apache.hadoop.hbase.index.mdhbase.MDPoint;
import org.apache.hadoop.hbase.index.mdhbase.MDUtils;
import org.apache.hadoop.hbase.index.userdefine.IndexPutParser;
import org.apache.hadoop.hbase.index.userdefine.IndexTableRelation;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.text.ParseException;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by winter on 17-3-11.
 */

public class PerfMD {

  private MDHBaseAdmin mdAdmin;
  private TableName opTableName;
  private TableName givenTableName;
  private IndexType indexType;
  AbstractWorkload workload;

  public PerfMD(TableName tableName, IndexType indexType, AbstractWorkload workload)
      throws IOException {
    this.givenTableName = tableName;
    this.indexType = indexType;
    this.workload = workload;
    mdAdmin = new MDHBaseAdmin(workload.getHBaseConfiguration(), givenTableName,
        workload.getMDBucketThreshold(), workload.getMDDimensions());
    if (indexType == IndexType.PF_MBKT || indexType == IndexType.PF_BKRW)
      opTableName = mdAdmin.getBucketTableName();
    else opTableName = mdAdmin.getSecondaryTableName();
    System.out.println("PerfNormal " + indexType + " on table " + opTableName);
  }

  public PerfInserterBase getPerfInserter(String loadDataDir, int processId, int threadNum,
      String statFilePath, ConcurrentLinkedQueue<String> reportQueue) throws IOException {
    return new PerfMDInserter(loadDataDir, processId, threadNum, statFilePath, reportQueue,
        workload);
  }

  public PerfScanBase getPerfScan(String dataDstDir, int nbGet, int nbScan, int sizeScanCovering,
      int testTimes) throws IOException {
    if (indexType == IndexType.PF_MBKT) {
      return new PerfMDBucketScan(indexType, workload, dataDstDir, nbGet, nbScan, sizeScanCovering,
          testTimes);
    } else if (indexType == IndexType.PF_BKRW) {
      return new PerfMDBucketReadScan(indexType, workload, dataDstDir, nbGet, nbScan,
          sizeScanCovering, testTimes);
    } else {
      return new PerfMDSecondaryScan(indexType, workload, dataDstDir, nbGet, nbScan,
          sizeScanCovering, testTimes);
    }
  }

  protected MDPoint getRecordMDPoint(String line) throws ParseException {
    AbstractDITBRecord record = workload.loadDITBRecord(line);
    return record.toMDPoint();
  }

  public class PerfMDInserter extends PerfInserterBase {

    private Admin admin;
    private Connection conn;
    private long gbuTime = 0;
    private long gbuCount = 0;

    public PerfMDInserter(String loadDataDir, int processId, int threadNum, String statFilePath,
        ConcurrentLinkedQueue<String> reportQueue, AbstractWorkload workload) throws IOException {
      super(workload.getHBaseConfiguration(), givenTableName, loadDataDir, processId, threadNum,
          statFilePath, reportQueue, workload);
      conn = ConnectionFactory.createConnection(conf);
      admin = conn.getAdmin();
    }

    @Override public void close() throws IOException {
      Closeables.closeQuietly(admin);
      Closeables.closeQuietly(conn);
      if (indexType == IndexType.PF_BKRW) {
        System.out.println(String
            .format("Get before update summary, %d get in %.2f seconds (avg lat=%.7f)", gbuCount,
                gbuTime / 1000.0, gbuTime / 1000.0 / gbuCount));
      }
    }

    @Override protected void checkTable(byte[][] splits) throws IOException {
      System.out.println("create table with " + (splits == null ? 0 : splits.length) + " regions");
      if (admin.tableExists(tableName)) {
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
      }
      HTableDescriptor desc = new HTableDescriptor(tableName);
      desc.addFamily(IndexTableRelation.getDefaultColumnDescriptor(workload.FAMILY_NAME));
      admin.createTable(desc, splits);
      mdAdmin.initTables(true);
    }

    @Override protected RunnablePerfInserter getProperDataInserter(int id, int reportInterval,
        ConcurrentLinkedQueue<AbstractDITBRecord> queue, FinishCounter fc) throws IOException {
      if (indexType == IndexType.PF_MBKT)
        return new RunnableMDBucketPerfInsert(id, reportInterval, queue, fc, mdAdmin);
      if (indexType == IndexType.PF_BKRW)
        return new RunnableMDBucketReadInsert(id, reportInterval, queue, fc, mdAdmin);
      else return new RunnableMDSecondaryPerfInsert(id, reportInterval, queue, fc, mdAdmin);
    }

    private class RunnableMDBucketPerfInsert extends RunnablePerfInserter {
      Table table;

      public RunnableMDBucketPerfInsert(int id, int reportInterval,
          ConcurrentLinkedQueue<AbstractDITBRecord> queue, FinishCounter fc, MDHBaseAdmin mdAdmin) {
        super(id, reportInterval, queue, fc);
        try {
          table = conn.getTable(opTableName);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }

      @Override protected void insertOneRecord(AbstractDITBRecord record) throws IOException {
        MDPoint point = record.toMDPoint();
        byte[] row = mdAdmin.getBucketSuffixRow(point);
        table.incrementColumnValue(row, MDHBaseAdmin.BUCKET_FAMILY,
            MDHBaseAdmin.BUCKET_SIZE_QUALIFIER, 1);
      }
    }

    private class RunnableMDBucketReadInsert extends RunnablePerfInserter {
      Table table;

      public RunnableMDBucketReadInsert(int id, int reportInterval,
          ConcurrentLinkedQueue<AbstractDITBRecord> queue, FinishCounter fc, MDHBaseAdmin mdAdmin) {
        super(id, reportInterval, queue, fc);
        try {
          table = conn.getTable(opTableName);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }

      @Override protected void insertOneRecord(AbstractDITBRecord record) throws IOException {
        // note, MD-HBase get before update, we summarize the time in get seperately
        MDPoint point = record.toMDPoint();
        byte[] row = MDUtils.bitwiseZip(point.values, mdAdmin.getDimensions());
        // get before row
        long startTime = System.currentTimeMillis();
        Scan scan = new Scan();
        scan.addFamily(MDHBaseAdmin.BUCKET_FAMILY);
        scan.setReversed(true);
        scan.setStartRow(row);
        scan.setCacheBlocks(false);
        scan.setCaching(1);
        scan.setSmall(true);
        ResultScanner scanner = table.getScanner(scan);
        Result result = scanner.next();
        scanner.close();
        gbuTime += System.currentTimeMillis() - startTime;
        gbuCount++;
        // default scan
        if (result == null) {
          row = mdAdmin.getBucketSuffixRow(point);
        } else {
          row = result.getRow();
        }
        table.incrementColumnValue(row, MDHBaseAdmin.BUCKET_FAMILY,
            MDHBaseAdmin.BUCKET_SIZE_QUALIFIER, 1);
      }
    }

    private class RunnableMDSecondaryPerfInsert extends RunnablePerfInserter {
      Table table;

      public RunnableMDSecondaryPerfInsert(int id, int reportInterval,
          ConcurrentLinkedQueue<AbstractDITBRecord> queue, FinishCounter fc, MDHBaseAdmin mdAdmin) {
        super(id, reportInterval, queue, fc);
        try {
          table = conn.getTable(opTableName);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }

      @Override protected void insertOneRecord(AbstractDITBRecord record) throws IOException {
        MDPoint point = record.toMDPoint();
        Put put = mdAdmin.getPutOnSecondary(point);
        table.put(put);
      }
    }
  }

  public class PerfMDBucketScan extends PerfScanBase {

    public PerfMDBucketScan(IndexType indexType, AbstractWorkload workload, String dataDstDir,
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
      MDPoint point = getRecordMDPoint(line);
      byte[] row = mdAdmin.getBucketSuffixRow(point);
      Get get = new Get(row);
      get.addFamily(MDHBaseAdmin.BUCKET_FAMILY);
      return get;
    }

    @Override protected boolean hasRandomGet() {
      return true;
    }

    @Override protected Result processGet(Table table, Get get) throws IOException {
      Scan scan = new Scan();
      scan.addFamily(MDHBaseAdmin.BUCKET_FAMILY);
      scan.setReversed(true);
      scan.setStartRow(get.getRow());
      scan.setCacheBlocks(false);
      scan.setCaching(1);
      scan.setSmall(true);
      ResultScanner scanner = table.getScanner(scan);
      Result ret = scanner.next();
      scanner.close();
      return ret;
    }

    @Override protected boolean hasSequenceGet() {
      return false;
    }

    @Override protected boolean hasScan() {
      return true;
    }

    @Override protected byte[] getIndexTableScanStartKey(String line)
        throws IOException, ParseException {
      MDPoint point = getRecordMDPoint(line);
      return mdAdmin.getBucketSuffixRow(point);
    }
  }

  public class PerfMDBucketReadScan extends PerfScanBase {

    public PerfMDBucketReadScan(IndexType indexType, AbstractWorkload workload, String dataDstDir,
        int nbGet, int nbScan, int sizeScanCovering, int testTimes) throws IOException {
      super(indexType, workload, dataDstDir, nbGet, nbScan, sizeScanCovering, testTimes);
    }

    @Override protected TableName getOpTableName() {
      return null;
    }

    @Override protected int recordsInOneResult(Result result) {
      throw new RuntimeException("should never be called");
    }

    @Override protected Get getIndexTableGet(String line) throws IOException, ParseException {
      throw new RuntimeException("should never be called");
    }

    @Override protected boolean hasRandomGet() {
      return false;
    }

    @Override protected boolean hasSequenceGet() {
      return false;
    }

    @Override protected boolean hasScan() {
      return false;
    }

    @Override protected byte[] getIndexTableScanStartKey(String line)
        throws IOException, ParseException {
      throw new RuntimeException("should never be called");
    }
  }

  public class PerfMDSecondaryScan extends PerfScanBase {

    public PerfMDSecondaryScan(IndexType indexType, AbstractWorkload workload, String dataDstDir,
        int nbGet, int nbScan, int sizeScanCovering, int testTimes) throws IOException {
      super(indexType, workload, dataDstDir, nbGet, nbScan, sizeScanCovering, testTimes);
    }

    @Override protected TableName getOpTableName() {
      return opTableName;
    }

    @Override protected int recordsInOneResult(Result result) {
      return result.listCells().size();
    }

    @Override protected boolean hasRandomGet() {
      return false;
    }

    @Override protected boolean hasSequenceGet() {
      return false;
    }

    @Override protected Get getIndexTableGet(String line) throws IOException, ParseException {
      throw new RuntimeException("should never be called");
    }

    @Override protected boolean hasScan() {
      return true;
    }

    protected byte[] getIndexTableScanStartKey(String line) throws IOException, ParseException {
      MDPoint point = getRecordMDPoint(line);
      return mdAdmin.getPutOnSecondary(point).getRow();
    }
  }

}
