package ditb.workload;

import ditb.util.DITBConstants;
import ditb.util.DITBUtil;
import ditb.ycsb.generator.NumberGenerator;
import ditb.ycsb.generator.UniformIntegerGenerator;
import ditb.ycsb.generator.ZipfianGenerator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.index.IndexType;
import org.apache.hadoop.hbase.index.client.DataType;
import org.apache.hadoop.hbase.index.mdhbase.MDPoint;
import org.apache.hadoop.hbase.index.mdhbase.MDRange;
import org.apache.hadoop.hbase.index.scanner.ScanRange;
import org.apache.hadoop.hbase.index.userdefine.IndexTableRelation;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by winter on 17-3-19.
 */
public class ExampleAWorkload extends RandomGeneratedWorkload {

  // table desc
  public final String[] indexColumnNames = new String[] { "a", "b" };
  // data column definition from workload desc File
  private int minAValue, maxAValue, minBValue, maxBValue;

  NumberGenerator colAGenerator;
  NumberGenerator colBGenerator;

  public ExampleAWorkload(String descFilePath) throws IOException {
    super(descFilePath);
    minAValue = loadIntParam("column.value.min.a", 0);
    maxAValue = loadIntParam("column.value.max.a", 10000);
    minBValue = loadIntParam("column.value.min.b", 0);
    maxBValue = loadIntParam("column.value.max.b", 10000);
    colAGenerator =
        getGenerator(loadStringParam("column.value.distribution.a", "latest"), minAValue,
            maxAValue);
    colBGenerator =
        getGenerator(loadStringParam("column.value.distribution.b", "uniform"), minBValue,
            maxBValue);
  }

  private NumberGenerator getGenerator(String desc, int minValue, int maxValue) {
    if (desc.equalsIgnoreCase("latest")) return new ZipfianGenerator(0, maxValue - minValue);
    else if (desc.equalsIgnoreCase("uniform"))
      return new UniformIntegerGenerator(0, maxValue - minValue);
    throw new RuntimeException("distribution not supported: " + desc);
  }

  @Override public IndexTableRelation getTableRelation(TableName tableName, IndexType indexType)
      throws IOException {
    IndexTableRelation relation = new IndexTableRelation(tableName, indexType);
    for (String col : indexColumnNames) {
      relation.addIndexColumn(FAMILY_NAME, Bytes.toBytes(col));
      relation.addColumn(FAMILY_NAME, Bytes.toBytes(col));
    }
    for (int i = ExampleARecord.DATA_COLUMN_OFFSET; i < nbTotalColumns; i++) {
      relation.addColumn(FAMILY_NAME, Bytes.toBytes(i));
    }
    return relation;
  }

  @Override public byte[][] getSplits() {
    if (nbRegion <= 1) return null;
    byte[][] splits = new byte[nbRegion][];
    byte[] right = Bytes.toBytes(0);
    for (int i = 0; i < nbRegion; i++) {
      splits[i] = Bytes.add(Bytes.toBytes(i), right);
    }
    return splits;
  }

  @Override public List<String> statDescriptions() {
    List<String> list = new ArrayList();
    list.add(String
        .format("a\t%s\t%d\t%d\t%d", DataType.INT.toString(), minAValue, maxAValue, nbLCStatRange));
    list.add(String
        .format("b\t%s\t%d\t%d\t%d", DataType.INT.toString(), minBValue, minBValue, nbLCStatRange));
    return list;
  }

  @Override public DataSource getDataSource() {
    return DataSource.RANDOM_GENE;
  }

  @Override public AbstractDITBRecord loadDITBRecord(String str) throws ParseException {
    return new ExampleARecord(str);
  }

  @Override public List<File> getSourceDataPath() {
    throw new RuntimeException(
        "no source data file in UniWorkload, which generates random records");
  }

  @Override public double getSelectRate() {
    throw new RuntimeException("no select rate in UniWorkload, which generates random records");
  }

  @Override public AbstractDITBRecord geneDITBRecord(Random random) {
    return geneDITBRecord(Math.abs(random.nextInt()), random);
  }

  @Override public AbstractDITBRecord geneDITBRecord(int key, Random random) {
    int a = minAValue + colAGenerator.nextValue().intValue();
    int b = minBValue + colBGenerator.nextValue().intValue();
    ExampleARecord record = new ExampleARecord(key, a, b, random);
    return record;
  }

  private byte[] getSaltRowkeyWithNbRegion(int rowkey) {
    int saltValue = new Integer(rowkey).hashCode() % nbRegion;
    return Bytes.add(Bytes.toBytes(saltValue), Bytes.toBytes(rowkey));
  }

  @Override public byte[] getRowkey(String rowkey) {
    return getSaltRowkeyWithNbRegion(Integer.valueOf(rowkey));
  }

  @Override public int getMDDimensions() {
    return 2;
  }

  @Override public MDRange[] getMDRanges(ScanRange.ScanRangeList rangeList) {
    MDRange[] ranges = new MDRange[2];
    for (ScanRange range : rangeList.getRanges()) {
      if (Bytes.toString(range.getQualifier()).equalsIgnoreCase("a")) {
        ranges[0] = DITBUtil.getMDRange(range);
      } else if (Bytes.toString(range.getQualifier()).equalsIgnoreCase("b")) {
        ranges[1] = DITBUtil.getMDRange(range);
      }
    }
    for (int i = 0; i < indexColumnNames.length; i++) {
      if (ranges[i] == null) {
        ranges[i] = new MDRange(0, Integer.MAX_VALUE);
      }
    }
    return ranges;
  }

  @Override public String parseResult(Result result) {
    return null;
  }

  private class ExampleARecord extends AbstractDITBRecord {
    private static final int DATA_COLUMN_OFFSET = 3;
    private final int rowkey;
    private final int a;
    private final int b;
    private final int[] columnValues; // [0, DATA_COLUMN_OFFSET-1] is not accessible

    public ExampleARecord(String line) {
      String[] splits = line.split(DITBConstants.DELIMITER);
      rowkey = Integer.valueOf(splits[0]);
      a = Integer.valueOf(splits[1]);
      b = Integer.valueOf(splits[2]);
      columnValues = new int[nbTotalColumns];
      for (int i = DATA_COLUMN_OFFSET; i < nbTotalColumns; i++) {
        columnValues[i] = Integer.valueOf(splits[i]);
      }
    }

    public ExampleARecord(int rowkey, int a, int b, Random random) {
      this.rowkey = rowkey;
      this.a = a;
      this.b = b;
      columnValues = new int[nbTotalColumns];
      for (int i = DATA_COLUMN_OFFSET; i < nbTotalColumns; i++) {
        columnValues[i] = random.nextInt();
      }
    }

    @Override public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(rowkey).append(DITBConstants.DELIMITER);
      sb.append(a).append(DITBConstants.DELIMITER);
      sb.append(b).append(DITBConstants.DELIMITER);
      for (int i = DATA_COLUMN_OFFSET; i < columnValues.length; i++) {
        sb.append(i).append(DITBConstants.DELIMITER).append(columnValues[i])
            .append(DITBConstants.DELIMITER);
      }
      return sb.toString();
    }

    @Override public Put getPut() {
      Put put = new Put(getSaltRowkey());
      put.addColumn(FAMILY_NAME, Bytes.toBytes("a"), Bytes.toBytes(a));
      put.addColumn(FAMILY_NAME, Bytes.toBytes("b"), Bytes.toBytes(b));
      for (int i = DATA_COLUMN_OFFSET; i < nbTotalColumns; i++) {
        put.addColumn(FAMILY_NAME, Bytes.toBytes(i), Bytes.toBytes(columnValues[i]));
      }
      return put;
    }

    @Override public MDPoint toMDPoint() {
      int[] arr = new int[] { a, b };
      return new MDPoint(getSaltRowkey(), arr);
    }

    @Override public String toLine() {
      StringBuilder sb = new StringBuilder();
      sb.append(rowkey).append(DITBConstants.DELIMITER);
      sb.append(a).append(DITBConstants.DELIMITER);
      sb.append(b).append(DITBConstants.DELIMITER);
      for (int i = DATA_COLUMN_OFFSET; i < columnValues.length; i++) {
        sb.append(columnValues[i]).append(DITBConstants.DELIMITER);
      }
      return sb.toString();
    }

    @Override public byte[] getRowkey() {
      return getSaltRowkey();
    }

    private byte[] getSaltRowkey() {
      return getSaltRowkeyWithNbRegion(rowkey);
    }
  }

}
