package org.apache.hadoop.hbase.index.userdefine;

import org.apache.hadoop.hbase.index.client.DataType;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by winter on 16-11-16.
 */
public class IndexRelationship implements Writable {
  private ColumnInfo mainColumn;
  private List<ColumnInfo> additionalColumns;

  /**
   * for writable
   */
  public IndexRelationship() {
  }

  /**
   * when the main column is given
   */
  public IndexRelationship(ColumnInfo mainColumn, List<ColumnInfo> list) {
    this.mainColumn = mainColumn;
    additionalColumns = list;
    if (list == null) {
      // init a blank array list for write and readFields
      additionalColumns = new ArrayList<>(0);
    }
  }

  public IndexRelationship(byte[] family, byte[] qualifier, DataType type, List<ColumnInfo> list) {
    mainColumn = new ColumnInfo(family, qualifier, type, true);
    additionalColumns = list;
  }

  @Override public void write(DataOutput out) throws IOException {
    mainColumn.write(out);
    WritableUtils.writeVInt(out, additionalColumns.size());
    for (ColumnInfo ci : additionalColumns) {
      ci.write(out);
    }
  }

  @Override public void readFields(DataInput in) throws IOException {
    mainColumn = new ColumnInfo();
    mainColumn.readFields(in);
    int size = WritableUtils.readVInt(in);
    additionalColumns = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      ColumnInfo ci = new ColumnInfo();
      ci.readFields(in);
      additionalColumns.add(ci);
    }
  }

  public ColumnInfo getMainColumn() {
    return mainColumn;
  }

  public List<ColumnInfo> getAdditionalColumns() {
    return additionalColumns;
  }
}
