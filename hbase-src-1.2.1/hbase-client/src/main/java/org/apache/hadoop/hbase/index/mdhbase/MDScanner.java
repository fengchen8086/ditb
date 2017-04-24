package org.apache.hadoop.hbase.index.mdhbase;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * Created by winter on 17-3-28.
 */
public class MDScanner implements ResultScanner {

  MDHBaseAdmin admin;
  LinkedList<MDPoint> candidates;
  Table rawTable;

  public MDScanner(MDHBaseAdmin admin, Table rawTable, MDRange[] ranges, int cacheSize) {
    this.admin = admin;
    this.rawTable = rawTable;
    try {
      candidates = admin.rangeQuery(ranges, cacheSize);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override public Result next() throws IOException {
    if (candidates == null || candidates.isEmpty()) return null;
    return rawTable.get(new Get(candidates.poll().id));
  }

  @Override public Result[] next(int nbRows) throws IOException {
    if (candidates == null || candidates.isEmpty()) return null;
    int size = Math.min(candidates.size(), nbRows);
    Result[] results = new Result[size];
    for (int i = 0; i < size; i++) {
      results[i] = rawTable.get(new Get(candidates.poll().id));
    }
    return results;
  }

  @Override public void close() {
    IOUtils.closeQuietly(admin);
    admin = null;
  }

  @Override public Iterator<Result> iterator() {
    return null;
  }
}
