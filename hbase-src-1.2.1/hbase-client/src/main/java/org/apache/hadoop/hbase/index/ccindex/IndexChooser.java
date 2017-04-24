package org.apache.hadoop.hbase.index.ccindex;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.index.IndexType;
import org.apache.hadoop.hbase.index.client.IndexConstants;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Choose an index to scan.
 *
 * @author liujia
 */
public abstract class IndexChooser {
  protected static final int DEFAULT_SPEED_TIMES = 10;
  protected final Log LOG = LogFactory.getLog(this.getClass());
  protected Map<byte[], List<HRegionInfo>> indexRegionMaps = null;
  protected IndexTable indexTable = null;
  //times which scan is faster than get
  protected int speedTimes = 1;

  public IndexChooser(final IndexTable indexTable) throws IOException {
    this.indexTable = indexTable;
    indexRegionMaps = new TreeMap<byte[], List<HRegionInfo>>(Bytes.BYTES_COMPARATOR);

    for (Map.Entry<byte[], Table> entry : indexTable.getIndexTableMaps().entrySet()) {
      if (!(entry.getValue() instanceof HTable)) {
        throw new IOException(
            "table is not an instance of HTable, it is " + entry.getValue().getClass().getName());
      }
      HTable htable = (HTable) entry.getValue();
      ArrayList<HRegionInfo> list =
          new ArrayList<HRegionInfo>(htable.getRegionLocations().keySet());
      indexRegionMaps.put(entry.getKey(), list);
    }

    speedTimes = DEFAULT_SPEED_TIMES;
  }

  public List<HRegionInfo> getHRegionInfo(byte[] indexColumn) {
    return indexRegionMaps.get(indexColumn);
  }

  /**
   * Check if you can get all resultColumns directly from scanning an index table.
   * If you can't, a secondary operation has to be performed if you want to get all resultColumns.
   *
   * @param indexColumn
   * @param resultColumns
   * @return true if you can get all result columns from the index table, otherwise false
   */
  public boolean containAllResultColumns(byte[] indexColumn, byte[][] resultColumns) {
    if (Bytes.compareTo(indexColumn, IndexConstants.KEY) == 0) {
      return true;
    }

    IndexTableDescriptor indexDesc = indexTable.getIndexTableDescriptor();
    IndexSpecification indexSpec = null;
    try {
      indexSpec = indexDesc.getIndexSpecification(indexColumn);
    } catch (IndexNotExistedException e) {
      LOG.error(e);
    }

    if (indexSpec.getIndexType() == IndexType.CCIndex) {
      return true;
    } else if (indexSpec.getIndexType() == IndexType.UDGIndex) {
      Map<byte[], Set<byte[]>> map = indexSpec.getAdditionMap();
      if (resultColumns == null) {
        Set<byte[]> families = indexDesc.getTableDescriptor().getFamiliesKeys();

        for (byte[] temp : families) {
          if (!map.containsKey(temp) || map.get(temp) != null) {
            return false;
          }
        }
      } else {
        for (byte[] temp : resultColumns) {
          if (Bytes.compareTo(temp, IndexConstants.KEY) == 0) {
            continue;
          }
          byte[][] parsetemp = KeyValue.parseColumn(temp);
          if (parsetemp.length == 1) {
            if (!map.containsKey(parsetemp[0]) || map.get(parsetemp[0]) != null) {
              return false;
            }
          } else {
            if (Bytes.compareTo(indexColumn, temp) == 0) {
              continue;
            }
            if (!map.containsKey(parsetemp[0]) || (map.get(parsetemp[0]) != null && !map
                .get(parsetemp[0]).contains(parsetemp[1]))) {
              return false;
            }
          }
        }
      }

    } else if (indexSpec.getIndexType() == IndexType.GSIndex) {
      if (resultColumns != null && resultColumns.length == 1 && (
          Bytes.compareTo(resultColumns[0], indexColumn) == 0
              || Bytes.compareTo(resultColumns[0], IndexConstants.KEY) == 0)) {
        return true;
      } else {
        return false;
      }
    }

    return true;
  }

  /**
   * Choose an index to scan, in order to get all resultColumns.
   *
   * @param ranges
   * @param resultColumns
   * @return
   * @throws IOException-when getting HRegion information failed
   */
  public abstract int whichToScan(Range[] ranges, byte[][] resultColumns) throws IOException;

}
