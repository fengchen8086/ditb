package ditb.hybrid;

import ditb.workload.AbstractDITBRecord;
import ditb.workload.AbstractWorkload;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.index.scanner.ScanRange;

import java.io.IOException;
import java.text.ParseException;

/**
 * Created by winter on 17-3-27.
 */
public abstract class Operation {
  public boolean isWrite() {
    return false;
  }

  public boolean isRead() {
    return false;
  }

  public boolean isScan() {
    return false;
  }

  public AbstractDITBRecord getRecord() {
    throw new RuntimeException("should be implemented");
  }

  public Get getGet() {
    throw new RuntimeException("should be implemented");
  }

  public Scan getScan() {
    throw new RuntimeException("should be implemented");
  }

  public static class WriteOperation extends Operation {
    public static final String OperationPrefix = "op-write";
    private AbstractDITBRecord record;

    public WriteOperation(AbstractDITBRecord record) {
      this.record = record;
    }

    @Override public boolean isWrite() {
      return true;
    }

    @Override public AbstractDITBRecord getRecord() {
      return record;
    }
  }

  public static class ReadOperation extends Operation {
    public static final String OperationPrefix = "op-read";
    private Get get;

    public ReadOperation(byte[] rowkey) {
      get = new Get(rowkey);
    }

    @Override public boolean isRead() {
      return true;
    }

    @Override public Get getGet() {
      return get;
    }
  }

  public static class ScanOperation extends Operation {
    public static final String OperationPrefix = "op-scan";
    private Scan scan;

    public ScanOperation(String fileName) throws IOException {
      scan = ScanRange.ScanRangeList.getScan(fileName);
      scan.setId(fileName);
    }

    @Override public boolean isScan() {
      return true;
    }

    @Override public Scan getScan() {
      return scan;
    }
  }

  public static Operation parseOperation(String line, AbstractWorkload workload)
      throws ParseException, IOException {
    if (line.startsWith(WriteOperation.OperationPrefix)) {
      line = line.substring(WriteOperation.OperationPrefix.length());
      AbstractDITBRecord record = workload.loadDITBRecord(line);
      return new WriteOperation(record);
    } else if (line.startsWith(ReadOperation.OperationPrefix)) {
      line = line.substring(ReadOperation.OperationPrefix.length());
      return new ReadOperation(workload.getRowkey(line));
    } else if (line.startsWith(ScanOperation.OperationPrefix)) {
      line = line.substring(ScanOperation.OperationPrefix.length());
      return new ScanOperation(line);
    }
    throw new RuntimeException("unknown line: " + line);
  }
}
