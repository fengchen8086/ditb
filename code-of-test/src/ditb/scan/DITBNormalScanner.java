package ditb.scan;

import ditb.workload.AbstractWorkload;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.index.IndexType;
import org.apache.hadoop.hbase.index.scanner.BaseIndexScanner;
import org.apache.hadoop.hbase.index.userdefine.IndexTableAdmin;
import org.apache.hadoop.hbase.index.userdefine.IndexTableRelation;

import java.io.IOException;

/**
 * Created by fengchen on 17-1-12.
 */
public class DITBNormalScanner extends DITBScanBase {

  IndexTableRelation relation;
  IndexTableAdmin admin;

  public DITBNormalScanner(TableName tableName, IndexType indexType, AbstractWorkload workload)
      throws IOException {
    super(tableName, indexType, workload.getHBaseConfiguration());
    admin = new IndexTableAdmin(conf, conn, tableName);
    relation =
        IndexTableRelation.getIndexTableRelation(admin.getTable(tableName).getTableDescriptor());
  }

  @Override public ResultScanner getScanner(Scan scan) throws IOException {
    return BaseIndexScanner.getIndexScanner(conn, relation, scan);
  }

  @Override public void close() throws IOException {
    admin.close();
    super.close();
  }
}
