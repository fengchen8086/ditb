package ditb.test;

import ditb.workload.AbstractWorkload;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

/**
 * Created by winter on 17-3-9.
 */
public class DoSomething {

  private final String workloadClsName = "ditb.workload.UniWorkload";
  private final String workloadDescPath =
      "/home/winter/workspace/git/LCIndex-HBase-1.2.1/scripts/ditb/conf/workload-uni";
  private final AbstractWorkload workload;
  private final Connection conn;
  private final TableName tableName = TableName.valueOf("tbl_speed_GSIndex-f-a-GSIndex");
  private final Admin admin;
  private final Table table;


  public DoSomething() throws IOException {
    workload = AbstractWorkload.getWorkload(workloadClsName, workloadDescPath);
    conn = ConnectionFactory.createConnection(workload.getHBaseConfiguration());
    admin = conn.getAdmin();
    table = conn.getTable(tableName);
  }

  private void work() throws IOException {
    admin.flush(tableName);
  }

  public static void main(String args[]) throws IOException {
    new DoSomething().work();
    System.out.println("done");
  }
}
