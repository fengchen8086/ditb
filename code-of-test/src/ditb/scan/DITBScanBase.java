package ditb.scan;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.index.IndexType;
import org.apache.hadoop.hbase.regionserver.HRegionServer;

import java.io.IOException;

/**
 * Created by fengchen on 17-1-12.
 */
public abstract class DITBScanBase {
  protected final TableName tableName;
  protected final Configuration conf;
  private final IndexType indexType;

  Connection conn;

  public DITBScanBase(TableName tableName, IndexType indexType, Configuration conf)
      throws IOException {
    this.tableName = tableName;
    this.indexType = indexType;
    this.conf = conf;
    conn = ConnectionFactory.createConnection(conf);
  }

  public abstract ResultScanner getScanner(Scan scan) throws IOException;

  public void close() throws IOException {
    IOUtils.closeQuietly(conn);
  }

}
