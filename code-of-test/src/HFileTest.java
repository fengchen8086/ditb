import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.index.client.DataType;
import org.apache.hadoop.hbase.index.mdhbase.MDUtils;
import org.apache.hadoop.hbase.index.userdefine.IndexPutParser;
import org.apache.hadoop.hbase.index.userdefine.IndexTableRelation;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Map;
import java.util.TreeSet;

/**
 * Created by winter on 16-12-14.
 */
public class HFileTest {

  Configuration conf;
  HTableDescriptor desc;
  TableName tableName = TableName.valueOf("tbl_lmd");
  Connection conn;
  IndexTableRelation relation;

  public HFileTest() throws IOException {
    conf = HBaseConfiguration.create();
    HRegionServer.loadWinterConf(conf, null);
    conn = ConnectionFactory.createConnection(conf);
    desc = conn.getTable(tableName).getTableDescriptor();
    relation = IndexTableRelation.getIndexTableRelation(desc);
  }

  private void doWork() throws IOException {
    //    String hdfsPath = "hdfs://localhost:9000/hbase/data/default/tbl_lmd/86caf56930b868f13bcd02cdf259edd2/.tmp/9a9d70c058084396bee0c59e808c9b36.lmd_data";
    String localPath = "/home/winter/tmp/4fd230622e1848ba8f3f01d3b02e277a.lmd_bucket";
    //        scanHFileOnHDFS(new Path(hdfsPath));
    scanHFileOnLocalFS(new Path(localPath));
  }

  private void scanHFileOnHDFS(Path hdfsPath) {
  }

  private void scanHFileOnLocalFS(Path file) throws IOException {
    HColumnDescriptor family = desc.getFamily(Bytes.toBytes("f"));
    CacheConfig cacheConf = new CacheConfig(conf, family);
    HFile.Reader reader = HFile.createReader(LocalFileSystem.getLocal(conf), file, cacheConf, conf);
    HFileScanner scanner = reader.getScanner(false, false, false);
    scanner.seekTo();
    int n = 0;
    do {
      Cell cell = scanner.getKeyValue();
      printKV(cell);
      ++n;
    } while (scanner.next());
  }

  private void printKV(Cell keyValue) {
    StringBuilder sb = new StringBuilder();
    sb.append("rowkey=" + Bytes.toStringBinary(keyValue.getRow()));
    int i = 0;
    int[] arr = MDUtils.bitwiseUnzip(keyValue.getRow(), 3);
    sb.append(", indicating=");
    for (Map.Entry<byte[], TreeSet<byte[]>> entry : relation.getIndexFamilyMap().entrySet()) {
      for (byte[] qualifer : entry.getValue()) {
        sb.append("[").append(Bytes.toString(entry.getKey())).append(":")
            .append(Bytes.toString(qualifer)).append("]=").append(arr[i]).append(",");
        ++i;
      }
    }
    sb.append(", rawRowkey=" + Bytes.toInt(keyValue.getQualifier()));
    System.out.println(sb.toString());
  }

  private DataType getDataType(byte[] qualifier) {
    if (Bytes.compareTo(qualifier, Bytes.toBytes("i")) == 0) return DataType.INT;
    if (Bytes.compareTo(qualifier, Bytes.toBytes("d")) == 0) return DataType.DOUBLE;
    if (Bytes.compareTo(qualifier, Bytes.toBytes("s")) == 0) return DataType.STRING;
    return null;
  }

  public static void main(String[] args) throws IOException {
    new HFileTest().doWork();
  }
}
