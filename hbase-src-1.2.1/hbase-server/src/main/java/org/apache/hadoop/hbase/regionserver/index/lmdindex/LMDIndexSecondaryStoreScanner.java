package org.apache.hadoop.hbase.regionserver.index.lmdindex;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by winter on 17-3-13.
 */
public class LMDIndexSecondaryStoreScanner {

  RegionScanner memstoreScanner;
  List<LMDIndexDirectStoreFileScanner> storeFileScanners;

  public LMDIndexSecondaryStoreScanner(RegionScanner memScanner,
      List<LMDIndexDirectStoreFileScanner> scanners) {
    this.memstoreScanner = memScanner;
    this.storeFileScanners = scanners;
  }

  public List<byte[]> getRowkeys() throws IOException {
    List<byte[]> rowkeyList = new ArrayList<>();
    while (true) {
      List<Cell> oneRow = new ArrayList<>();
      if (!memstoreScanner.nextRaw(oneRow)) break;
      rowkeyList.add(oneRow.get(0).getRow());
    }
    for (LMDIndexDirectStoreFileScanner scanner : storeFileScanners) {
      rowkeyList.addAll(scanner.getRowkeyList());
    }
    return rowkeyList;
  }

}
