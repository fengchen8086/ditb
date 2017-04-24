package org.apache.hadoop.hbase.index.userdefine;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.stat.descriptive.SynchronizedMultivariateSummaryStatistics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.index.IndexType;
import org.apache.hadoop.hbase.index.client.DataType;
import org.apache.hadoop.hbase.index.client.IndexColumnDescriptor;
import org.apache.hadoop.hbase.index.client.IndexDescriptor;
import org.apache.hadoop.hbase.index.scanner.BaseIndexScanner;

import java.io.IOException;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

/**
 * Index Table Admin, it combines the functions of both HBaseAdmin and TableDescriptor,
 * Each table contains only one IndexTableAdmin
 * used to
 * 1. create/disable/drop table,
 * 2. put rows with client side index
 * 3. get rows with rowkey
 * 4. scan rows by rowkey
 * 5. scan rows by index columns
 * 6. update rows by rowkey
 * 7. delete rows by rowkey
 * Created by winter on 16-11-16.
 */
public class IndexTableAdmin {
  private static final Log LOG = LogFactory.getLog(IndexTableAdmin.class);
  private final boolean buildFromTableDesc;
  private Configuration conf = null;
  private Connection conn = null;
  private Admin admin = null;
  private IndexTableRelation indexTableRelation;
  private IndexPutParser putParser;
  private Map<TableName, Table> tableMap = new Hashtable<>();
  private Table rawTable = null;
  private String lcIndexRange = null;
  private int lmdThreshold = -1;

  private long buildIndexTotalTime = 0;
  private long insertIndexTotalTime = 0;
  private long insertRawTableTotalTime = 0;
  private long nbPuts = 0;

  /**
   * init table, table indexType will be given manually
   * used for existing table
   *
   * @param conf
   * @param conn
   * @param tableName
   * @throws IOException
   */
  public IndexTableAdmin(Configuration conf, Connection conn, TableName tableName)
      throws IOException {
    this.conf = conf;
    this.conn = conn;
    this.admin = conn.getAdmin();
    HTableDescriptor desc = admin.getTableDescriptor(tableName);
    this.indexTableRelation = IndexTableRelation.getIndexTableRelation(desc);
    buildFromTableDesc = true;
    init();
  }

  /**
   * init table by indexTableRelation,
   * used for non-existing table
   *
   * @param conf
   * @param conn
   * @param indexTableRelation
   * @throws IOException
   */
  public IndexTableAdmin(Configuration conf, Connection conn, IndexTableRelation indexTableRelation)
      throws IOException {
    this.conf = conf;
    this.conn = conn;
    this.admin = conn.getAdmin();
    this.indexTableRelation = indexTableRelation;
    buildFromTableDesc = false;
    init();
  }

  private void init() throws IOException {
    if (IndexType.isLocalIndex(indexTableRelation.getIndexType())) putParser = null;
    else
      putParser = IndexPutParser.getParser(indexTableRelation.getIndexType(), indexTableRelation);
  }

  public void createTable(boolean dropIfExists, boolean checkIndexes) throws IOException {
    createTable(dropIfExists, checkIndexes, null);
  }

  /**
   * create table for indexes
   *
   * @param dropIfExists drop table if exists then create
   * @param checkIndexes if drop not enabled, check index relations, drop if not matching
   * @throws IOException
   */
  public void createTable(boolean dropIfExists, boolean checkIndexes, byte[][] splits)
      throws IOException {
    if (indexTableRelation.getIndexType() == IndexType.LCIndex && lcIndexRange == null)
      throw new IllegalIndexException(indexTableRelation.getTableName(),
          indexTableRelation.getIndexType(), "must set lcindex range");
    boolean shouldDrop = dropIfExists;
    TableName tableName = indexTableRelation.getTableName();
    HTableDescriptor desc = null;
    if (admin.tableExists(tableName)) {
      desc = admin.getTableDescriptor(tableName);
      if (!shouldDrop && checkIndexes) {
        // different from the index information on from desc given
        shouldDrop = !buildFromTableDesc && !indexTableRelation
            .equals(IndexTableRelation.getIndexTableRelation(desc));
      }
      if (!shouldDrop) {
        return;
      } else {
        dropIndexTables();
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
      }
    }
    // index tables
    createIndexTables();
    desc = new HTableDescriptor(tableName);
    for (byte[] family : indexTableRelation.getFamilyMap().keySet()) {
      // LCTODO: fix it later, IndexColumnDescriptor must be removed!
      if (indexTableRelation.getIndexType() == IndexType.IRIndex) {
        IndexColumnDescriptor indexColumnDescriptor =
            new IndexColumnDescriptor(family, IndexType.IRIndex);
        for (byte[] qualifier : indexTableRelation.getIndexFamilyMap().get(family)) {
          IndexDescriptor index = new IndexDescriptor(qualifier, DataType.STRING);
          indexColumnDescriptor.addIndex(index);
        }
        desc.addFamily(indexColumnDescriptor);
      } else {
        desc.addFamily(IndexTableRelation.getDefaultColumnDescriptor(family));
      }
    }
    desc.setValue(IndexTableRelation.INDEX_ATTRIBUTE_NAME_BYTES,
        indexTableRelation.getIndexAttribute());
    if (indexTableRelation.getIndexType() == IndexType.LCIndex) {
      desc.setValue("LC_DATA_RANGE", lcIndexRange); // because LCIndexConstants is in server side
      // desc.setValue(LCIndexConstant.LC_TABLE_DESC_RANGE_STR, lcIndexRange);
    } else if (indexTableRelation.getIndexType() == IndexType.CCIndex) {
      desc.setValue(IndexType.CCIT_REPLICATION_FACTOR_ATTRIBUTE, String.valueOf(1));
    } else if (indexTableRelation.getIndexType() == IndexType.LMDIndex_D
        || indexTableRelation.getIndexType() == IndexType.LMDIndex_S) {
      if (lmdThreshold != -1) {
        desc.setValue("lmdindex.bucket.size.threshold", String.valueOf(lmdThreshold));
      }
    }
    System.out.println("creating table: " + desc.getTableName());
    admin.createTable(desc, splits);
  }

  /**
   * create index tables for global indexes
   */
  private void createIndexTables() throws IOException {
    if (indexTableRelation.isGlobalIndex()) {
      List<HTableDescriptor> tables = indexTableRelation.getIndexTableDescriptors();
      for (HTableDescriptor desc : tables) {
        LOG.info("creating table " + desc.getTableName() + " for index " + indexTableRelation
            .getIndexType());
        admin.createTable(desc);
      }
    } else {
      LOG.info("there's no additional table for index " + indexTableRelation.getIndexType());
    }
  }

  /**
   * drop index tables for global indexes
   *
   * @throws IOException
   */
  private void dropIndexTables() throws IOException {
    if (indexTableRelation.isGlobalIndex()) {
      List<HTableDescriptor> tables = indexTableRelation.getIndexTableDescriptors();
      for (HTableDescriptor desc : tables) {
        LOG.info(
            "disable and delete table " + desc.getTableName() + " for index " + indexTableRelation
                .getIndexType());
        admin.disableTable(desc.getTableName());
        admin.deleteTable(desc.getTableName());
      }
    } else {
      LOG.info("there's no additional table for index " + indexTableRelation.getIndexType());
    }
  }

  public void putWithIndex(Put put) throws IOException {
    if (putParser != null) {
      // build index and put index values
      long startTime = System.currentTimeMillis();
      Map<TableName, Put> putMap = putParser.parsePut(put);
      long middle = System.currentTimeMillis();
      for (Map.Entry<TableName, Put> entry : putMap.entrySet()) {
        getTable(entry.getKey()).put(entry.getValue());
      }
      buildIndexTotalTime += middle - startTime;
      insertIndexTotalTime += System.currentTimeMillis() - middle;
    }
    ++nbPuts;
    // add the raw table put
    if (rawTable == null) rawTable = conn.getTable(indexTableRelation.getTableName());
    long startTime = System.currentTimeMillis();
    rawTable.put(put);
    insertRawTableTotalTime += System.currentTimeMillis() - startTime;
  }

  public void putWithIndex(List<Put> puts) throws IOException {
    if (putParser != null) {
      long startTime = System.currentTimeMillis();
      Map<TableName, List<Put>> putMap = putParser.parsePutList(puts);
      long middle = System.currentTimeMillis();
      for (Map.Entry<TableName, List<Put>> entry : putMap.entrySet()) {
        getTable(entry.getKey()).put(entry.getValue());
      }
      buildIndexTotalTime += middle - startTime;
      insertIndexTotalTime += System.currentTimeMillis() - middle;
    }
    nbPuts += puts.size();
    // add the raw table put
    if (rawTable == null) rawTable = conn.getTable(indexTableRelation.getTableName());
    long startTime = System.currentTimeMillis();
    rawTable.put(puts);
    insertRawTableTotalTime += System.currentTimeMillis() - startTime;
  }

  public Result getWithoutIndex(Get get) throws IOException {
    if (rawTable == null) rawTable = conn.getTable(indexTableRelation.getTableName());
    return rawTable.get(get);
  }

  public Table getTable(TableName tableName) throws IOException {
    Table table = tableMap.get(tableName);
    if (table == null) {
      table = conn.getTable(tableName);
      tableMap.put(tableName, table);
    }
    return table;
  }

  public void close() throws IOException {
    if (tableMap != null) {
      for (Table table : tableMap.values()) {
        table.close();
      }
      tableMap = null;
    }
    if (admin != null) {
      admin.close();
      admin = null;
    }
    System.out.println(String.format(
        "%s summary, %d puts, %.2f seconds for raw table (avg lat=%.7f), %.2f seconds for index table (avg lat=%.7f), %.2f seconds for building index (avg lat=%.7f)",
        indexTableRelation.getIndexType().toString(), nbPuts, insertRawTableTotalTime / 1000.0,
        insertRawTableTotalTime / 1000.0 / nbPuts, insertIndexTotalTime / 1000.0,
        insertIndexTotalTime / 1000.0 / nbPuts, buildIndexTotalTime / 1000.0,
        buildIndexTotalTime / 1000.0 / nbPuts));
  }

  public BaseIndexScanner getScanner(Scan scan) throws IOException {
    return BaseIndexScanner.getIndexScanner(conn, indexTableRelation, scan);
  }

  public void setLCIndexRange(String lcIndexRange) {
    this.lcIndexRange = lcIndexRange;
  }

  public void setLMDIndexThreshold(int threshold) {
    this.lmdThreshold = threshold;
  }
}
