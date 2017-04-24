/*
 *    Copyright 2012 Shoji Nishimura
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.hbase.index.mdhbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Bucket is a container of points. Each bucket is associated with a sub-space
 * organized by Index.
 * Schema:
 * <ul>
 * <li>row key: bitwise zip of {@link MDPoint#values}. Take two dimensions(x,y) as example,
 * x=[x0,x1,..,x31], y=[y0,y1,..,y31] -> [x0,y0,x1,y1,..,x31,y31]
 * <li>column name: "P:" + byte array of {@link MDPoint#id}
 * <li>column value: concatination of byte arrays of {@link MDPoint#values} and
 * {@link MDPoint#values}
 * </ul>
 *
 * @author shoji
 */
public class MDBucket {

  private final Table secondaryTable;
  private final byte[] startRow;
  private final byte[] stopRow;
  private final MDIndex mdIndex;
  private final MDRange[] ranges;
  public long lastScanTotalTime = 0;
  public int lastScanTotalNumber = 0;

  public MDBucket(Table dataTable, MDRange[] ranges, MDIndex mdIndex) {
    checkNotNull(dataTable);
    checkNotNull(ranges);
    for (MDRange r : ranges) {
      checkNotNull(r);
    }
    checkNotNull(mdIndex);
    this.secondaryTable = dataTable;
    this.ranges = ranges;
    int[] mins = new int[ranges.length];
    int[] maxs = new int[ranges.length];
    for (int i = 0; i < ranges.length; i++) {
      mins[i] = ranges[i].min;
      maxs[i] = ranges[i].max;
    }
    this.startRow = MDUtils.bitwiseZip(mins, mdIndex.dimensions);
    this.stopRow = MDUtils.increment(MDUtils.bitwiseZip(maxs, mdIndex.dimensions));
    this.mdIndex = mdIndex;
  }

  public void insert(byte[] row, MDPoint p) throws IOException {
    Put put = new Put(row);
    put.addColumn(MDHBaseAdmin.SECONDARY_FAMILY, p.toQualifier(), p.toValue());
    secondaryTable.put(put);
    mdIndex.notifyInsertion(row, 1);
  }

  public void insert(byte[] row, List<MDPoint> points) throws IOException {
    Put put = new Put(row);
    for (MDPoint p : points) {
      put.addColumn(MDHBaseAdmin.SECONDARY_FAMILY, p.toQualifier(), p.toValue());
    }
    secondaryTable.put(put);
    mdIndex.notifyInsertion(row, points.size());
  }

  /**
   * gets points at the query points
   *
   * @param row
   * @return
   * @throws IOException
   */
  public Collection<MDPoint> get(byte[] row) throws IOException {
    Get get = new Get(row);
    get.addFamily(MDHBaseAdmin.SECONDARY_FAMILY);
    Result result = secondaryTable.get(get);
    List<MDPoint> found = new LinkedList<>();
    transformResultAndAddToList(result, found);
    return found;
  }

  /**
   * scans this bucket and retrieves all points within the query region.
   *
   * @param ranges query ranges on dimensions
   * @return a collection of points within the query region
   * @throws IOException
   */
  public Collection<MDPoint> scan(MDRange[] ranges, int cacheSize) throws IOException {
    Scan scan = new Scan(startRow, stopRow);
    MDRangeFilter filter = new MDRangeFilter(ranges);
    //    scan.setFilter(filter);
    scan.setCaching(cacheSize);
    ResultScanner scanner = secondaryTable.getScanner(scan);
    List<MDPoint> results = new LinkedList<>();
    long startTime = System.currentTimeMillis();
    int counter = 0;
    for (Result result : scanner) {
      ++counter;
      Filter.ReturnCode code = Filter.ReturnCode.INCLUDE;
      for (Cell cell : result.listCells()) {
        code = filter.filterKeyValue(cell);
        if (code != Filter.ReturnCode.INCLUDE) break;
      }
      if (code == Filter.ReturnCode.INCLUDE) transformResultAndAddToList(result, results);
    }
    lastScanTotalNumber = counter;
    lastScanTotalTime = System.currentTimeMillis() - startTime;
    scanner.close();
    return results;
  }

  public Collection<MDPoint> scan(int cacheSize) throws IOException {
    return scan(ranges, cacheSize);
  }

  private void transformResultAndAddToList(Result result, List<MDPoint> found) {
    NavigableMap<byte[], byte[]> map = result.getFamilyMap(MDHBaseAdmin.SECONDARY_FAMILY);
    if (map == null) return;
    for (Entry<byte[], byte[]> entry : map.entrySet()) {
      MDPoint p = toPoint(entry.getKey(), entry.getValue());
      found.add(p);
    }
  }

  private MDPoint toPoint(byte[] qualifier, byte[] value) {
    int[] array = new int[mdIndex.dimensions];
    for (int i = 0; i < array.length; i++) {
      array[i] = Bytes.toInt(value, i * 4);
    }
    return new MDPoint(qualifier, array);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  @Override public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Bucket [");
    for (MDRange r : ranges) {
      sb.append("(").append(r.min).append(",").append(r.max).append("),");
    }
    sb.append("]");
    return sb.toString();
  }
}
