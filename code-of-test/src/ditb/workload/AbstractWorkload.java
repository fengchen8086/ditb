package ditb.workload;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.index.IndexType;
import org.apache.hadoop.hbase.index.mdhbase.MDRange;
import org.apache.hadoop.hbase.index.scanner.ScanRange;
import org.apache.hadoop.hbase.index.userdefine.IndexTableRelation;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Created by winter on 17-3-7.
 */
public abstract class AbstractWorkload {

  private static final Log LOG = LogFactory.getLog(AbstractWorkload.class);

  public static final byte[] FAMILY_NAME = Bytes.toBytes("f");

  protected String descFilePath;
  protected Map<String, String> paramMap;

  protected int nbRegion;
  protected int nbLCStatRange;
  private String scanFilterDir;
  private int scanRunTimes;
  private int scanCacheSize;
  private TableName tableName;
  protected Configuration conf;
  private int mdBucketThreshold;
  protected int nbTotalColumns;
  protected String skipFilterPrefix;
  protected int nbExistingRows;

  public AbstractWorkload(String descFilePath) throws IOException {
    this.descFilePath = descFilePath;
    // load parameters from descFile
    paramMap = new HashMap<>();
    loadParamsFromFile();
    // init params
    nbRegion = loadIntParam("region.number", 10);
    nbLCStatRange = loadIntParam("lcindex.range.number", 1000);
    scanFilterDir = loadStringParam("scan.filter.dir", "/tmp/should-assign");
    scanRunTimes = loadIntParam("scan.run.times", 1);
    scanCacheSize = loadIntParam("scan.cache.size", 100);
    mdBucketThreshold = loadIntParam("md.bucket.threshold", 1000);
    tableName = TableName.valueOf(loadStringParam("table.name", "tbl_x"));
    nbTotalColumns = loadIntParam("total.columns.number", 20);
    nbExistingRows = loadIntParam("number.existing.rows", 100);
    skipFilterPrefix = loadStringParam("skip.filter.prefix", "skip");
    // init hbase conf
    conf = HBaseConfiguration.create();
    if (paramMap.containsKey("hbase.conf.path")) conf.addResource(paramMap.get("hbase.conf.path"));
    if (paramMap.containsKey("hbase.conf.path.additional"))
      HRegionServer.loadWinterConf(conf, paramMap.get("hbase.conf.path.additional"));
  }

  @Override public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(", nbRegion=").append(nbRegion);
    sb.append(", nbLCStatRange=").append(nbLCStatRange);
    sb.append(", scanFilterDir=").append(scanFilterDir);
    sb.append(", scanRunTimes=").append(scanRunTimes);
    sb.append(", scanCacheSize=").append(scanCacheSize);
    sb.append(", tableName=").append(tableName);
    return sb.toString();
  }

  private void loadParamsFromFile() throws IOException {
    File file = new File(descFilePath);
    if (!file.exists() || !file.isFile()) {
      LOG.info("file " + descFilePath + " not exist or not a file");
    } else {
      BufferedReader br = new BufferedReader(new FileReader(file));
      String line;
      while ((line = br.readLine()) != null) {
        if (line.startsWith("#")) continue;
        LOG.info("conf file " + descFilePath + " param: " + line);
        String[] splits = line.split("=");
        paramMap.put(splits[0], splits[1]);
      }
    }
  }

  public Configuration getHBaseConfiguration() {
    return conf;
  }

  protected int loadIntParam(String desc, int defaultValue) {
    if (paramMap.containsKey(desc)) defaultValue = Integer.valueOf(paramMap.get(desc));
    LOG.info("load " + desc + "=" + defaultValue);
    return defaultValue;
  }

  protected double loadDoubleParam(String desc, double defaultValue) {
    if (paramMap.containsKey(desc)) defaultValue = Double.valueOf(paramMap.get(desc));
    LOG.info("load " + desc + "=" + defaultValue);
    return defaultValue;
  }

  protected String loadStringParam(String desc, String defaultValue) {
    if (paramMap.containsKey(desc)) defaultValue = paramMap.get(desc);
    LOG.info("load " + desc + "=" + defaultValue);
    return defaultValue;
  }

  public static AbstractWorkload getWorkload(String className, String descFilePath)
      throws IOException {
    try {
      Class cls = Class.forName(className);
      Class[] paraTypes = new Class[] { String.class };
      Object[] params = new Object[] { descFilePath };
      Constructor con = cls.getConstructor(paraTypes);
      AbstractWorkload workload = (AbstractWorkload) con.newInstance(params);
      return workload;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public List<File> getScanFilterFiles() {
    List<File> list = new ArrayList<>();
    for (File file : new File(scanFilterDir).listFiles()) {
      if (file.isDirectory() || file.getName().startsWith(skipFilterPrefix)) continue;
      list.add(file);
    }
    Collections.sort(list, new Comparator<File>() {
      @Override public int compare(File o1, File o2) {
        return o1.getName().compareTo(o2.getName());
      }
    });
    return list;
  }

  public abstract IndexTableRelation getTableRelation(TableName tableName, IndexType indexType)
      throws IOException;

  public abstract byte[][] getSplits();

  public abstract List<String> statDescriptions();

  public abstract DataSource getDataSource();

  public abstract AbstractDITBRecord loadDITBRecord(String str) throws ParseException;

  public abstract List<File> getSourceDataPath();

  public abstract double getSelectRate();

  public abstract AbstractDITBRecord geneDITBRecord(Random random);

  public abstract AbstractDITBRecord geneDITBRecord(int rowkey, Random random);

  public abstract int getTotalOperations();

  public abstract int getMDDimensions();

  public abstract MDRange[] getMDRanges(ScanRange.ScanRangeList rangeList);

  public int getScanRunTimes() {
    return scanRunTimes;
  }

  public int getScanCacheSize() {
    return scanCacheSize;
  }

  public TableName getTableName() {
    return tableName;
  }

  public abstract String parseResult(Result result);

  public int getMDBucketThreshold() {
    return mdBucketThreshold;
  }

  public int getTotalColumns() {
    return nbTotalColumns;
  }

  public int getNbExistingRows() {
    return nbExistingRows;
  }

  public abstract byte[] getRowkey(String rowkey);

  //  public abstract Get getFromLine(String line);

  //  public abstract Scan scanFromLine(String line);

  public enum DataSource {
    FROM_DISK, RANDOM_GENE
  }
}
