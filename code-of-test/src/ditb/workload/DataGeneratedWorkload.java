package ditb.workload;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by winter on 17-3-21.
 */
public abstract class DataGeneratedWorkload extends AbstractWorkload {
  protected double selectRate;
  protected String sourceFileDir;
  protected String sourceFilePrefix;

  public DataGeneratedWorkload(String descFilePath) throws IOException {
    super(descFilePath);
    selectRate = loadDoubleParam("select.rate", 0.1);
    sourceFileDir = loadStringParam("data.source.dir", "/tmp/should-assign-manually");
    sourceFilePrefix = loadStringParam("source.file.prefix", "attempt");
  }

  @Override public DataSource getDataSource() {
    return DataSource.FROM_DISK;
  }

  @Override public AbstractDITBRecord geneDITBRecord(Random random) {
    throw new RuntimeException("no geneDITBRecord in AdWorkload, which loads data from disk");
  }

  @Override public AbstractDITBRecord geneDITBRecord(int rowkey, Random random) {
    throw new RuntimeException("no geneDITBRecord in AdWorkload, which loads data from disk");
  }

  @Override public int getTotalOperations() {
    throw new RuntimeException("no recordPerFile in AdWorkload, which loads data from disk");
  }

  @Override public byte[] getRowkey(String rowkey) {
    throw new RuntimeException("not implemented");
  }

  @Override public double getSelectRate() {
    return selectRate;
  }

  @Override public List<File> getSourceDataPath() {
    List<File> list = new ArrayList<>();
    for (File file : new File(sourceFileDir).listFiles()) {
      if (file.isDirectory()) continue;
      if (file.getName().startsWith(sourceFilePrefix)) list.add(file);
    }
    return list;
  }
}
