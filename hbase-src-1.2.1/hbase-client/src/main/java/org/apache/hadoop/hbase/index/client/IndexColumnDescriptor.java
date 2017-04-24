package org.apache.hadoop.hbase.index.client;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.index.IndexType;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.ColumnFamilySchema;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

/**
 * IndexTableDescriptor holds additional index information of a table.
 * An index is defined on a column, and each column has no more than one index.
 */
public class IndexColumnDescriptor extends HColumnDescriptor {
  private static final byte[] INDEX = Bytes.toBytes("INDEX");
  private static final byte[] INDEX_TYPE = Bytes.toBytes("INDEX_TYPE");
  // public static final byte[]

  // all IndexDescriptors
  private final Map<byte[], IndexDescriptor> indexes = new TreeMap<>(Bytes.BYTES_COMPARATOR);
  TreeMap<byte[], DataType> qualifierTypeMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
  private IndexType indexType = null;

  public IndexColumnDescriptor() {
  }

  public IndexColumnDescriptor(byte[] familyName, IndexType typeValue) {
    super(familyName);
    indexType = typeValue;
  }

  public IndexColumnDescriptor(HColumnDescriptor desc) {
    super(desc);
    if (desc instanceof IndexColumnDescriptor) {
      System.out.println("winter add new IndexColumnDescriptor because of instanceof");
      IndexColumnDescriptor indexDesc = (IndexColumnDescriptor) desc;
      addIndexes(indexDesc.getAllIndex());
    } else {
      byte[] bytes = desc.getValue(INDEX);
      if (bytes != null && bytes.length != 0) {
        DataInputBuffer indexin = new DataInputBuffer();
        indexin.reset(bytes, bytes.length);
        int size;
        try {
          indexType = typeOfIndexValue(indexin.readInt());
          size = indexin.readInt();
          for (int i = 0; i < size; i++) {
            IndexDescriptor indexDescriptor = new IndexDescriptor();
            indexDescriptor.readFields(indexin);
            indexes.put(indexDescriptor.getQualifier(), indexDescriptor);
            qualifierTypeMap.put(indexDescriptor.getQualifier(), indexDescriptor.getType());
          }
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  private static int valueOfIndexType(IndexType t) {
    if (t == IndexType.IRIndex) return 1;
    if (t == IndexType.LCIndex) return 2;
    if (t == IndexType.CCIndex) return 3;
    return 0;
  }

  private static IndexType typeOfIndexValue(int i) {
    if (i == 1) return IndexType.IRIndex;
    if (i == 2) return IndexType.LCIndex;
    if (i == 3) return IndexType.CCIndex;
    return IndexType.NoIndex;
  }

  public Map<byte[], IndexDescriptor> getAllIndexMap() {
    return Collections.unmodifiableMap(indexes);
  }

  public IndexDescriptor[] getAllIndex() {
    return indexes.values().toArray(new IndexDescriptor[0]);
  }

  public IndexDescriptor getIndex(byte[] qualifier) {
    if (!indexes.containsKey(qualifier)) {
      return null;
    }
    return indexes.get(qualifier);
  }

  /**
   * Add an IndexDescriptor to table descriptor.
   *
   * @param index
   */
  public void addIndex(IndexDescriptor index) {
    if (index != null && index.getQualifier() != null) {
      indexes.put(index.getQualifier(), index);
      qualifierTypeMap.put(index.getQualifier(), index.getType());
    }
  }

  /**
   * Add IndexDescriptors to table descriptor.
   *
   * @param indexes
   */
  public void addIndexes(IndexDescriptor[] indexes) {
    if (indexes != null && indexes.length != 0) {
      for (IndexDescriptor index : indexes) {
        addIndex(index);
      }

    }
  }

  /**
   * Delete an Index from table descriptor.
   *
   * @param family
   */
  public void deleteIndex(byte[] qualifier) {
    if (indexes.containsKey(qualifier)) {
      indexes.remove(qualifier);
    }
  }

  /**
   * Delete all IndexSpecifications.
   *
   * @throws IOException
   */
  public void deleteAllIndex() throws IOException {
    indexes.clear();
  }

  /**
   * Check if table descriptor contains any index.
   *
   * @return true if has index
   */
  public boolean hasIndex() {
    return !indexes.isEmpty();
  }

  public IndexType getIndexType() {
    return indexType;
  }

  /**
   * Check if table descriptor contains the specific index.
   *
   * @return true if has index
   */
  public boolean containIndex(byte[] qualifier) {
    return indexes.containsKey(qualifier);
  }

  @Override public void readFields(DataInput in) throws IOException {
    super.readFields(in);

    DataInputBuffer indexin = new DataInputBuffer();
    byte[] bytes = super.getValue(INDEX);
    indexin.reset(bytes, bytes.length);

    int size = indexin.readInt();
    for (int i = 0; i < size; i++) {
      IndexDescriptor indexDescriptor = new IndexDescriptor();
      indexDescriptor.readFields(indexin);
      indexes.put(indexDescriptor.getQualifier(), indexDescriptor);
    }
  }

  @Override public void write(DataOutput out) throws IOException {
    DataOutputBuffer indexout = new DataOutputBuffer();
    System.out.println(
        "winter write indexColumn descripter, indexType is: " + valueOfIndexType(indexType));
    indexout.writeInt(valueOfIndexType(indexType));
    indexout.writeInt(indexes.size());
    for (IndexDescriptor indexDescriptor : indexes.values()) {
      indexDescriptor.write(indexout);
    }
    super.setValue(INDEX, indexout.getData());
    super.write(out);
  }

  @Override
  /**
   * @return Convert this instance to a the pb column family type
   */ public ColumnFamilySchema convert() {
    try {
      DataOutputBuffer indexout = new DataOutputBuffer();
      // System.out.println("winter write indexColumn descripter, indexType is: "
      // + valueOfIndexType(indexType));
      indexout.writeInt(valueOfIndexType(indexType));
      indexout.writeInt(indexes.size());
      for (IndexDescriptor indexDescriptor : indexes.values()) {
        indexDescriptor.write(indexout);
      }
      super.setValue(INDEX, indexout.getData());
    } catch (IOException e1) {
      e1.printStackTrace();
    }
    return super.convert();
  }

  // get the <qualifier - type> mapping
  public TreeMap<byte[], DataType> getQualifierTypeMap() {
    return qualifierTypeMap;
  }
}
