package org.apache.hadoop.hbase.index.ccindex;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Index is added before, but now it is missing!
 *
 * @author wanhao
 */
public class IndexMissingException extends IOException {
  private static final long serialVersionUID = 3287276074162104910L;
  private TableName tableName;
  private IndexSpecification indexSpec;

  public IndexMissingException(TableName tableName, IndexSpecification indexSpec) {
    super("Index [column:" + Bytes.toString(indexSpec.getIndexColumn()) + ",type:" + indexSpec
        .getIndexType().toString() + "]" + " is missing!"
        + "Please delete it first before performing any other operations on this table!");
    this.tableName = tableName;
    this.indexSpec = indexSpec;
  }

  public TableName getTableName() {
    return tableName;
  }

  public IndexSpecification getIndex() {
    return indexSpec;
  }
}
