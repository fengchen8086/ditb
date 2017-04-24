/*
 * Copyright 2012 Shoji Nishimura
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hbase.index.mdhbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Tiny MD-HBase MDHBaseAdmin
 *
 * @author shoji
 */
public class MDHBaseAdmin implements Closeable {

  private static final String BUCKET_TABLE_SUFFIX = "_md_bucket";
  private static final String SECONDARY_TABLE_SUFFIX = "_md_secondary";
  public static final byte[] BUCKET_FAMILY = "info".getBytes();
  public static final byte[] SECONDARY_FAMILY = "f".getBytes();
  public static final byte[] BUCKET_SIZE_QUALIFIER = "bs".getBytes();
  public static final byte[] BUCKET_PREFIX_LEN_QUALIFIER = "pl".getBytes();
  private final MDIndex index;
  private final int dimensions;
  private final int bucketThresholdSuffix;
  private final TableName rawTableName;
  private final TableName bucketTableName;
  private final TableName secondaryTableName;

  public MDHBaseAdmin(Configuration conf, TableName rawTableName, int splitThreshold,
      int dimensions) throws IOException {
    this.rawTableName = rawTableName;
    this.bucketTableName = TableName.valueOf(rawTableName.getNameAsString() + BUCKET_TABLE_SUFFIX);
    this.secondaryTableName =
        TableName.valueOf(rawTableName.getNameAsString() + SECONDARY_TABLE_SUFFIX);
    this.dimensions = dimensions;
    this.bucketThresholdSuffix = MDUtils.getSuffix(splitThreshold);
    this.index = new MDIndex(conf, bucketTableName, secondaryTableName, splitThreshold, dimensions);
  }

  public void initTables(boolean dropIfExists) throws IOException {
    index.createTable(dropIfExists);
  }

  public void insert(MDPoint p) throws IOException {
    byte[] row = MDUtils.bitwiseZip(p.values, dimensions);
    MDBucket bucket = index.fetchBucket(row);
    bucket.insert(row, p);
  }

  public void insert(List<MDPoint> points) throws IOException {
    TreeMap<byte[], List<MDPoint>> map = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for (MDPoint point : points) {
      byte[] row = MDUtils.bitwiseZip(point.values, dimensions);
      List<MDPoint> list = map.get(row);
      if (list == null) {
        list = new ArrayList<>();
        map.put(row, list);
      }
      list.add(point);
    }

    for (Map.Entry<byte[], List<MDPoint>> entry : map.entrySet()) {
      MDBucket bucket = index.fetchBucket(entry.getKey());
      bucket.insert(entry.getKey(), entry.getValue());
    }
  }

  public Iterable<MDPoint> get(int[] array) throws IOException {
    byte[] row = MDUtils.bitwiseZip(array, dimensions);
    MDBucket bucket = index.fetchBucket(row);
    return bucket.get(row);
  }

  /**
   * @param ranges query ranges on dimensions
   * @return points within the query region
   * @throws IOException
   */
  public LinkedList<MDPoint> rangeQuery(MDRange[] ranges, int cacheSize) throws IOException {
    long indexTotalTime = 0, bucketTotalTime = 0;
    int indexTotalNumber = 0, bucketTotalNumber = 0;
    Iterable<MDBucket> buckets = index.findBucketsInRange(ranges, cacheSize);
    indexTotalNumber = index.lastScanTotalNumber;
    indexTotalTime = index.lastScanTotalTime;
    LinkedList<MDPoint> results = new LinkedList<>();
    for (MDBucket bucket : buckets) {
      results.addAll(bucket.scan(ranges, cacheSize));
      bucketTotalNumber = bucket.lastScanTotalNumber;
      bucketTotalTime = bucket.lastScanTotalTime;
    }
    System.out.println(String.format(
        "MDHBase range scan, index table scan costs %.2f seconds for %d records (avg %.7f), bucket scan costs %.2f seconds for %d records (avg %.7f)",
        indexTotalTime / 1000.0, indexTotalNumber, indexTotalNumber / (indexTotalTime / 1000.0),
        bucketTotalTime / 1000.0, bucketTotalNumber,
        bucketTotalNumber / (bucketTotalTime / 1000.0)));
    return results;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.io.Closeable#close()
   */
  @Override public void close() throws IOException {
    index.close();
  }

  public TableName getRawTableName() {
    return rawTableName;
  }

  public TableName getBucketTableName() {
    return bucketTableName;
  }

  public TableName getSecondaryTableName() {
    return secondaryTableName;
  }

  public byte[] getBucketSuffixRow(MDPoint p) {
    byte[] row = MDUtils.bitwiseZip(p.values, dimensions);
    MDUtils.maskSuffix(row, bucketThresholdSuffix);
    return row;
  }

  public Put getPutOnSecondary(MDPoint p) {
    byte[] row = MDUtils.bitwiseZip(p.values, dimensions);
    Put put = new Put(row);
    put.addColumn(SECONDARY_FAMILY, p.toQualifier(), p.toValue());
    return put;
  }

  public int getDimensions() {
    return dimensions;
  }
}
