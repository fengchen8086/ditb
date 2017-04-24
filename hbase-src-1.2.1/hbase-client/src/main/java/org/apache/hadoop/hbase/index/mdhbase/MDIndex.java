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

import com.google.common.io.Closeables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.index.userdefine.IndexTableRelation;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * Index
 * Index maintains partitioned spaces. When the number of points in a sub-space
 * exceeds a split threshold, the index halves the sub-space and allocates two
 * new buckets for the partitioned sub-spaces.
 * Schema:
 * <ul>
 * <li>row key: min key of a bucket
 * <li>column family: info
 * <ul>
 * <li>column: pl, common prefix length of points in a bucket
 * <li>column: bs, size of a bucket/number of points in a bucket
 * </ul>
 * </ul>
 * Bucket name, which is named after the common prefix naming scheme is
 * represented as a pair of binary value and its prefix length. For example.
 * [011*****] is represented as a pair of [01100000] and 3.
 *
 * @author shoji
 */
public class MDIndex implements Closeable {

  public long lastScanTotalTime = 0;
  public int lastScanTotalNumber = 0;

  private final int splitThreshold;
  public final int dimensions;
  private Configuration config;
  private Table secondaryTable;
  private Table bucketTable;
  private Admin admin;
  private long prevRowTotalTime = 0;
  private long prevRowTotalCount = 0;
  private long incBucketTotalTime = 0;
  private long incBucketTotalCount = 0;
  private long splitTimes = 0;

  private final TableName bucketTableName;
  private final TableName secondaryTableName;

  Connection conn;

  public MDIndex(Configuration config, TableName bucketTableName, TableName secondaryTableName,
      int splitThreshold, int dimensions) throws IOException {
    this.config = config;
    this.bucketTableName = bucketTableName;
    this.secondaryTableName = secondaryTableName;
    this.splitThreshold = splitThreshold;
    this.dimensions = dimensions;
    conn = ConnectionFactory.createConnection(config);
    admin = conn.getAdmin();
    createTable(false);
  }

  /**
   * @param dropIfExists
   */
  public void createTable(boolean dropIfExists) throws IOException {
    if (admin.tableExists(secondaryTableName)) {
      if (dropIfExists) {
        admin.disableTable(bucketTableName);
        admin.deleteTable(bucketTableName);
        admin.disableTable(secondaryTableName);
        admin.deleteTable(secondaryTableName);
      } else {
        secondaryTable = conn.getTable(secondaryTableName);
        bucketTable = conn.getTable(bucketTableName);
        return;
      }
    }
    // secondary table
    HTableDescriptor secondaryDesc = new HTableDescriptor(secondaryTableName);
    secondaryDesc
        .addFamily(IndexTableRelation.getDefaultColumnDescriptor(MDHBaseAdmin.SECONDARY_FAMILY));
    admin.createTable(secondaryDesc);
    secondaryTable = conn.getTable(secondaryTableName);
    // bucket table
    HTableDescriptor bucketDesc = new HTableDescriptor(bucketTableName);
    bucketDesc.addFamily(IndexTableRelation.getDefaultColumnDescriptor(MDHBaseAdmin.BUCKET_FAMILY));
    admin.createTable(bucketDesc);
    bucketTable = conn.getTable(bucketTableName);
    // init when init
    int[] starts = new int[dimensions];
    Arrays.fill(starts, 0);
    Put put = new Put(MDUtils.bitwiseZip(starts, dimensions));
    put.addColumn(MDHBaseAdmin.BUCKET_FAMILY, MDHBaseAdmin.BUCKET_PREFIX_LEN_QUALIFIER,
        Bytes.toBytes(dimensions));
    put.addColumn(MDHBaseAdmin.BUCKET_FAMILY, MDHBaseAdmin.BUCKET_SIZE_QUALIFIER,
        Bytes.toBytes(0L));
    bucketTable.put(put);
  }

  /**
   * fetches a bucket which holds the queried row.
   *
   * @param row a queried row key
   * @return a bucket which holds the queried row.
   * @throws IOException
   */
  public MDBucket fetchBucket(byte[] row) throws IOException {
    Result bucketEntry = getRowOrBefore(bucketTable, row, MDHBaseAdmin.BUCKET_FAMILY);
    byte[] bucketKey = bucketEntry.getRow();
    // prefix length has been written while creating table, defined to #dimension
    int prefixLength = Bytes.toInt(
        bucketEntry.getValue(MDHBaseAdmin.BUCKET_FAMILY, MDHBaseAdmin.BUCKET_PREFIX_LEN_QUALIFIER));
    MDRange[] ranges = toRanges(bucketKey, prefixLength);
    return createBucket(ranges);
  }

  private MDRange[] toRanges(byte[] bucketKey, int prefixLength) {
    // substitute don't cares to 0s. ex. [010*****] -> [01000000]
    int[] mins = MDUtils.bitwiseUnzip(bucketKey, dimensions);
    // tmp = "0b 11100000"
    byte[] tmp = MDUtils.makeMask(prefixLength, dimensions);
    // suffix_ones = "0b 0001111"
    byte[] suffix_ones = MDUtils.not(tmp);
    // substitute don't cares to 1s. ex. [010*****] -> [01011111]
    int[] maxs = MDUtils.bitwiseUnzip(MDUtils.or(bucketKey, suffix_ones), dimensions);
    MDRange[] ranges = new MDRange[dimensions];
    for (int i = 0; i < dimensions; i++) {
      ranges[i] = new MDRange(mins[i], maxs[i]);
    }
    return ranges;
  }

  /**
   * finds buckets which intersect with the query region.
   *
   * @param ranges
   * @return
   * @throws IOException
   */
  public Iterable<MDBucket> findBucketsInRange(MDRange[] ranges, int cacheSize) throws IOException {
    int[] mins = new int[ranges.length];
    int[] maxs = new int[ranges.length];
    for (int i = 0; i < ranges.length; i++) {
      mins[i] = ranges[i].min;
      maxs[i] = ranges[i].max;
    }
    byte[] probeKey = MDUtils.bitwiseZip(mins, dimensions);
    Result bucketEntry = getRowOrBefore(bucketTable, probeKey, MDHBaseAdmin.BUCKET_FAMILY);
    byte[] startKey = bucketEntry.getRow();
    //    byte[] stopKey = Bytes.incrementBytes(MDUtils.bitwiseZip(maxs), 1L);
    byte[] stopKey = MDUtils.increment(MDUtils.bitwiseZip(maxs, dimensions));
    Scan scan = new Scan(startKey, stopKey);
    scan.addFamily(MDHBaseAdmin.BUCKET_FAMILY);
    scan.setCaching(cacheSize);
    ResultScanner scanner = bucketTable.getScanner(scan);
    List<MDBucket> hitBuckets = new LinkedList<>();
    long startTime = System.currentTimeMillis();
    int counter = 0;
    for (Result result : scanner) {
      ++counter;
      byte[] row = result.getRow();
      int pl = Bytes.toInt(
          result.getValue(MDHBaseAdmin.BUCKET_FAMILY, MDHBaseAdmin.BUCKET_PREFIX_LEN_QUALIFIER));
      MDRange[] rs = toRanges(row, pl);
      boolean intersect = true;
      for (int i = 0; i < rs.length; i++) {
        if (!ranges[i].intersect(rs[i])) {
          intersect = false;
          break;
        }
      }
      if (intersect) {
        hitBuckets.add(createBucket(rs));
      }
    }
    lastScanTotalNumber = counter;
    lastScanTotalTime = System.currentTimeMillis() - startTime;
    scanner.close();
    return hitBuckets;
  }

  private MDBucket createBucket(MDRange[] rs) {
    return new MDBucket(secondaryTable, rs, this);
  }

  /**
   * @param row
   * @throws IOException
   */
  void notifyInsertion(byte[] row, long newAdded) throws IOException {
    Result bucketEntry = getRowOrBefore(bucketTable, row, MDHBaseAdmin.BUCKET_FAMILY);
    byte[] bucketKey = bucketEntry.getRow();
    long startTime = System.currentTimeMillis();
    long size = bucketTable.incrementColumnValue(bucketKey, MDHBaseAdmin.BUCKET_FAMILY,
        MDHBaseAdmin.BUCKET_SIZE_QUALIFIER, newAdded);
    incBucketTotalCount++;
    incBucketTotalTime += System.currentTimeMillis() - startTime;
    maySplit(bucketKey, size);
  }

  private void maySplit(byte[] bucketKey, long size) throws IOException {
    if (size > splitThreshold) {
      splitBucket(bucketKey);
    }
  }

  /*
   * bucket [abc*****] is partitioned into bucket [abc0****] and bucket
   * [abc1****].
   */
  private void splitBucket(byte[] splitKey) throws IOException {
    Result bucketEntry = getRowOrBefore(bucketTable, splitKey, MDHBaseAdmin.BUCKET_FAMILY);
    byte[] bucketKey = bucketEntry.getRow();
    int prefixLength = Bytes.toInt(
        bucketEntry.getValue(MDHBaseAdmin.BUCKET_FAMILY, MDHBaseAdmin.BUCKET_PREFIX_LEN_QUALIFIER));
    long bucketSize = Bytes.toLong(
        bucketEntry.getValue(MDHBaseAdmin.BUCKET_FAMILY, MDHBaseAdmin.BUCKET_SIZE_QUALIFIER));
    int newPrefixLength = prefixLength + 1;
    if (newPrefixLength > 32 * 2) {
      return; // exceeds the maximum prefix length.
    }
    byte[] newChildKey0 = bucketKey;
    byte[] newChildKey1 = MDUtils.makeBit(bucketKey, prefixLength, dimensions);
    Scan scan = new Scan(newChildKey0, newChildKey1);
    scan.addFamily(MDHBaseAdmin.SECONDARY_FAMILY);
    scan.setCaching(1000);
    ResultScanner scanner = secondaryTable.getScanner(scan);
    long newSize = 0L;
    for (Result result : scanner) {
      newSize += result.getFamilyMap(MDHBaseAdmin.SECONDARY_FAMILY).size();
    }
    splitTimes++;
    scanner.close();
    Put put0 = new Put(newChildKey0);
    put0.addColumn(MDHBaseAdmin.BUCKET_FAMILY, MDHBaseAdmin.BUCKET_PREFIX_LEN_QUALIFIER,
        Bytes.toBytes(newPrefixLength));
    put0.addColumn(MDHBaseAdmin.BUCKET_FAMILY, MDHBaseAdmin.BUCKET_SIZE_QUALIFIER,
        Bytes.toBytes(newSize));
    Put put1 = new Put(newChildKey1);
    put1.addColumn(MDHBaseAdmin.BUCKET_FAMILY, MDHBaseAdmin.BUCKET_PREFIX_LEN_QUALIFIER,
        Bytes.toBytes(newPrefixLength));
    put1.addColumn(MDHBaseAdmin.BUCKET_FAMILY, MDHBaseAdmin.BUCKET_SIZE_QUALIFIER,
        Bytes.toBytes(bucketSize - newSize));
    List<Put> puts = new ArrayList<>(2);
    puts.add(put0);
    puts.add(put1);
    bucketTable.put(puts);
    maySplit(newChildKey0, newSize);
    maySplit(newChildKey1, bucketSize - newSize);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.io.Closeable#close()
   */
  @Override public void close() throws IOException {
    Closeables.closeQuietly(secondaryTable);
    Closeables.closeQuietly(bucketTable);
    Closeables.closeQuietly(admin);
    Closeables.closeQuietly(conn);
    System.out.println(String
        .format("MDIndex bucket get prev, %.5f seconds for %d results, avg latency %.7f",
            prevRowTotalTime / 1000.0, prevRowTotalCount,
            prevRowTotalTime / 1000.0 / prevRowTotalCount));
    System.out.println(String
        .format("MDIndex bucket increase value , %.5f seconds for %d times, avg latency: %.7f",
            incBucketTotalTime / 1000.0, splitTimes,
            incBucketTotalTime / 1000.0 / incBucketTotalCount));
  }

  public Result getRowOrBefore(Table table, byte[] row, byte[] family) throws IOException {
    long start = System.currentTimeMillis();
    Scan scan = new Scan();
    scan.addFamily(family);
    scan.setReversed(true);
    scan.setStartRow(row);
    scan.setCacheBlocks(false);
    scan.setCaching(1);
    scan.setSmall(true);
    ResultScanner scanner = table.getScanner(scan);
    Result ret = scanner.next();
    scanner.close();
    prevRowTotalTime += System.currentTimeMillis() - start;
    prevRowTotalCount++;
    return ret;
  }
}
