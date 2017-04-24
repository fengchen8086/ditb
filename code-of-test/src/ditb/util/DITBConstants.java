package ditb.util;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.index.IndexType;
import org.apache.hadoop.hbase.index.userdefine.IndexTableRelation;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DITBConstants {


  // public static final String CONF_FILE = "/home/winter/scripts/scripts/uni/conf/winter-hbase.conf";

  public static final String REMOTE_PUTTING_MSG = "REMOTE_IS_PUTTING_MSG";
  public static final String REMOTE_PUTTING_REPORT_MSG = "REMOTE_PUTTING_REPORT_MSG";
  public static final String REMOTE_PUT_DONE_MSG = "REMOTE_PUT_DONE_MSG";
  public static final byte[] NO_MEANS_BYTES = Bytes.toBytes("hi,shifen");

  // client and server communication
  public static final int REMOTE_SERVER_PORT = 18756;
  public static final int REMOTE_FILE_TRANS_BUFFER_LEN = 500 * 1000; // 500k

  public static final String DELIMITER = "\t";

  public enum RemotePutStatus {
    SEND_PARAM, CHECK_DATA, CHECK_TABLE, START_TO_WORK, FLUSH, CLOSE, SAY_HI
  }

}
