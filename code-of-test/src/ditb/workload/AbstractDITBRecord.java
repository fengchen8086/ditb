package ditb.workload;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.index.mdhbase.MDPoint;

/**
 * Created by winter on 17-3-7.
 */

public abstract class AbstractDITBRecord {

  public abstract String toString();

  public abstract Put getPut();

  public abstract MDPoint toMDPoint();

  public abstract String toLine();

  public abstract byte[] getRowkey();
}