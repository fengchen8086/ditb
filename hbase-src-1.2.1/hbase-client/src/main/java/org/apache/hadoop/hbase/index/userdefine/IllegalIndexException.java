package org.apache.hadoop.hbase.index.userdefine;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.index.IndexType;

import java.io.IOException;

/**
 * Created by winter on 16-11-16.
 */
public class IllegalIndexException extends IOException {
  private IndexType indexType;
  private IndexRelationship indexRelationship;

  public IllegalIndexException(TableName tableName, IndexType indexType,
      IndexRelationship relationship) {
    super("table " + tableName + " with index type " + indexType
        + " meets illegal index relationship " + relationship);
  }

  public IllegalIndexException(TableName tableName, IndexType indexType, String msg) {
    super(
        "table " + tableName + " with index type " + indexType + " has exception, msg is: " + msg);
  }
}
