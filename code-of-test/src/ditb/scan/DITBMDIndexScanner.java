package ditb.scan;

import ditb.workload.AbstractWorkload;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.index.IndexType;
import org.apache.hadoop.hbase.index.mdhbase.MDHBaseAdmin;
import org.apache.hadoop.hbase.index.mdhbase.MDRange;
import org.apache.hadoop.hbase.index.mdhbase.MDScanner;
import org.apache.hadoop.hbase.index.scanner.ScanRange;

import java.io.IOException;

/**
 * Created by fengchen on 17-1-12.
 */
public class DITBMDIndexScanner extends DITBScanBase {
  MDHBaseAdmin mdAdmin;
  Table rawTable;
  AbstractWorkload workload;

  public DITBMDIndexScanner(TableName tableName, IndexType indexType, AbstractWorkload workload)
      throws IOException {
    super(tableName, indexType, workload.getHBaseConfiguration());
    mdAdmin = new MDHBaseAdmin(conf, tableName, workload.getMDBucketThreshold(),
        workload.getMDDimensions());
    rawTable = conn.getTable(tableName);
    this.workload = workload;
  }

  @Override public ResultScanner getScanner(Scan scan) throws IOException {
    ScanRange.ScanRangeList rangeList = ScanRange.ScanRangeList.getScanRangeList(scan);
    MDRange[] ranges = workload.getMDRanges(rangeList);
    return new MDScanner(mdAdmin, rawTable, ranges, workload.getScanCacheSize());
  }

  @Override public void close() throws IOException {
    IOUtils.closeQuietly(mdAdmin);
    super.close();
  }
}
