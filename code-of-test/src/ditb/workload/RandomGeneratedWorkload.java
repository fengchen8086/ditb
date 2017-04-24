package ditb.workload;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Created by winter on 17-3-21.
 */
public abstract class RandomGeneratedWorkload extends AbstractWorkload {
  protected int nbTotalOperations;

  public RandomGeneratedWorkload(String descFilePath) throws IOException {
    super(descFilePath);
    nbTotalOperations = loadIntParam("total.operation.number", 1000);
  }

  @Override public int getTotalOperations() {
    return nbTotalOperations;
  }

  @Override public DataSource getDataSource() {
    return DataSource.RANDOM_GENE;
  }

  @Override public List<File> getSourceDataPath() {
    throw new RuntimeException(
        "no source data file in UniWorkload, which generates random records");
  }

  @Override public double getSelectRate() {
    throw new RuntimeException("no select rate in UniWorkload, which generates random records");
  }
}
