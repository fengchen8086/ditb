package org.apache.hadoop.hbase.index.userdefine;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.index.IndexType;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Index Table Admin, it combines the functions of both HBaseAdmin and TableDescriptor,
 * Each table contains only one IndexTableAdmin
 * used to
 * 1. create/disable/drop table,
 * 2. put rows with client side index
 * 3. get rows with rowkey
 * 4. scan rows by rowkey
 * 5. scan rows by index columns
 * 6. update rows by rowkey
 * 7. delete rows by rowkey
 * Created by winter on 16-11-16.
 */
public class IndexTableRelation implements Writable {

  public static final byte[] INDEX_ATTRIBUTE_NAME_BYTES = Bytes.toBytes("INDEX_ATTR_BYTES");
  private static final Log LOG = LogFactory.getLog(IndexTableRelation.class);
  private final byte[] TABLE_SEPARATOR = Bytes.toBytes("-");
  // suffix, used to generate index table name
  private TableName tableName;
  private IndexType indexType;
  // attributes saves index information(type and relationship), be null until used
  private byte[] indexAttribute = null;
  // saves table descriptors, be null until used
  private List<HTableDescriptor> tableDescriptors;
  private TreeMap<byte[], TreeSet<byte[]>> familyMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);

  /**
   * table format: rk, f:idxA, f:idxB, f:info(not index)
   * 1. in secondary index, indexFamilyMap is [f - [idxA, idxB]], indexRelations is null, indexColumnMap is null
   * 2. in clustering index, indexFamilyMap is [f - [idxA, idxB]], indexRelations is null,
   * indexColumnMap is [idxA -> idxB], [idxB -> idxA], [info -> [idxA, idxB]], means idxA is used in table of idxB, info is included in both tables.
   * 3. in user-defined index, indexRelations is given by users to describe the index structure, for example:
   * indexRelations = [idxA -> null (secondary)], [idxB -> [idxA, info] (clustering)],
   * indexColumnMap = [idxA -> idxB], [info -> idxB]. means idxA and info is also included in table of idxB.
   * then, indexFamilyMap is automatically built according to indexRelations, = [idxA, idxB]
   */
  private List<IndexRelationship> indexRelations;
  private TreeMap<byte[], TreeSet<byte[]>> indexFamilyMap = null;
  private Map<ColumnInfo, Set<ColumnInfo>> indexColumnMap = null;

  /**
   * init table, table indexType will be given manually
   */
  public IndexTableRelation(TableName tableName, IndexType iType) throws IOException {
    this.tableName = tableName;
    this.indexType = iType;
  }

  public IndexTableRelation(TableName tableName, IndexType iType, IndexTableRelation another)
      throws IOException {
    this.tableName = tableName;
    this.indexType = iType;
    this.familyMap = another.familyMap;
    this.indexRelations = another.getIndexRelations();
    this.indexFamilyMap = another.indexFamilyMap;
    this.indexColumnMap = another.indexColumnMap;
  }

  public static IndexTableRelation getIndexTableRelation(HTableDescriptor desc) throws IOException {
    byte[] bytes;
    if (desc != null && (bytes = desc.getValue(INDEX_ATTRIBUTE_NAME_BYTES)) != null) {
      ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
      DataInputStream dis = new DataInputStream(inputStream);
      IndexTableRelation rl = new IndexTableRelation(desc.getTableName(), IndexType.NoIndex);
      rl.readFields(dis);
      return rl;
    }
    LOG.info("index table relation = null, because " + (desc == null ?
        "desc is null" :
        ("attribute is null for table " + desc.getTableName())));
    return null;
  }

  public TableName getTableName() {
    return tableName;
  }

  public IndexType getIndexType() {
    return indexType;
  }

  /**
   * add column into family map, all columns must be added by this way
   *
   * @param family
   * @param qualifier
   */
  public void addColumn(byte[] family, byte[] qualifier) {
    TreeSet<byte[]> set = familyMap.get(family);
    if (set == null) {
      set = new TreeSet<>(Bytes.BYTES_COMPARATOR);
      familyMap.put(family, set);
    }
    set.add(qualifier);
  }

  /**
   * set column family map directly instead of adding them one by one
   *
   * @param one
   */
  public void setColumns(TreeMap<byte[], TreeSet<byte[]>> one) {
    familyMap = one;
  }

  /**
   * adding index column
   *
   * @param family
   * @param qualifier
   * @throws IllegalIndexException when there's no index or UserDefined Index
   */
  public void addIndexColumn(byte[] family, byte[] qualifier) {
    if (indexType == IndexType.NoIndex || IndexType.isUserDefinedIndex(indexType)) {
      LOG.info("should not assign index column for " + indexType + " skip");
    }
    if (indexFamilyMap == null) indexFamilyMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    TreeSet<byte[]> set = indexFamilyMap.get(family);
    if (set == null) {
      set = new TreeSet<>(Bytes.BYTES_COMPARATOR);
      indexFamilyMap.put(family, set);
    }
    set.add(qualifier);
  }

  /**
   * set index columns directly instead of adding them one by one
   *
   * @param one
   */
  public void setIndexColumns(TreeMap<byte[], TreeSet<byte[]>> one) {
    if (indexType == IndexType.NoIndex || IndexType.isUserDefinedIndex(indexType)) {
      LOG.info("should not assign index column for " + indexType + " skip");
    }
    indexFamilyMap = one;
  }

  /**
   * get index table descriptors
   *
   * @return
   */
  public List<HTableDescriptor> getIndexTableDescriptors() {
    if (tableDescriptors != null) return tableDescriptors;
    tableDescriptors = new ArrayList<>();
    switch (indexType) {
    case CCIndex:
    case GCIndex:
      for (Map.Entry<byte[], TreeSet<byte[]>> entry : indexFamilyMap.entrySet()) {
        byte[] f = entry.getKey();
        for (byte[] qualifier : entry.getValue()) {
          HTableDescriptor desc = new HTableDescriptor(getIndexTableName(f, qualifier));
          HTableDescriptor cctDesc = indexType == IndexType.CCIndex ?
              new HTableDescriptor(getCCTName(f, qualifier)) :
              null;
          // cc/gc has all column families
          for (byte[] family : familyMap.keySet()) {
            desc.addFamily(getDefaultColumnDescriptor(family));
            if (indexType == IndexType.CCIndex) { // init cct families
              cctDesc.addFamily(getDefaultColumnDescriptor(family));
            }
          }
          tableDescriptors.add(desc);
          if (indexType == IndexType.CCIndex) {
            desc.setValue(IndexType.CCIT_REPLICATION_FACTOR_ATTRIBUTE, String.valueOf(1));
            tableDescriptors.add(cctDesc);
          }
        }
      }
      break;
    case GSIndex:
      for (Map.Entry<byte[], TreeSet<byte[]>> entry : indexFamilyMap.entrySet()) {
        byte[] f = entry.getKey();
        for (byte[] qualifier : entry.getValue()) {
          HTableDescriptor desc = new HTableDescriptor(getIndexTableName(f, qualifier));
          // GS only contains one family, whose values are always null/blank
          desc.addFamily(getDefaultColumnDescriptor(IndexType.SEDONDARY_FAMILY_BYTES));
          tableDescriptors.add(desc);
        }
      }
      break;
    case UDGIndex:
      for (IndexRelationship relation : indexRelations) {
        ColumnInfo ci = relation.getMainColumn();
        // use a set to avoid duplication
        TreeSet<byte[]> familySet = new TreeSet<>(Bytes.BYTES_COMPARATOR);
        List<ColumnInfo> additionalColumns = relation.getAdditionalColumns();
        if (additionalColumns == null || additionalColumns.isEmpty()) {
          // it is an secondary index, no other columns are included
          familySet.add(IndexType.SEDONDARY_FAMILY_BYTES);
        } else {
          for (ColumnInfo additionalColumn : relation.getAdditionalColumns()) {
            familySet.add(additionalColumn.getFamily());
          }
        }
        // the table descriptor,
        HTableDescriptor desc =
            new HTableDescriptor(getIndexTableName(ci.getFamily(), ci.getQualifier()));
        for (byte[] f : familySet) { // with families from additionalColumn
          desc.addFamily(getDefaultColumnDescriptor(f));
        }
        tableDescriptors.add(desc);
      }
    }
    return tableDescriptors;
  }

  public boolean isGlobalIndex() {
    return indexType == IndexType.CCIndex || indexType == IndexType.GCIndex
        || indexType == IndexType.GSIndex || indexType == IndexType.UDGIndex;
  }

  /**
   * create index attribute, used to put into HTableDescriptor
   *
   * @return
   */
  public byte[] getIndexAttribute() throws IOException {
    if (indexAttribute == null) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(baos);
      write(dos);
      dos.flush();
      baos.flush();
      indexAttribute = baos.toByteArray();
    }
    return indexAttribute;
  }

  /**
   * write index attributes
   *
   * @param out
   * @throws IOException
   */
  @Override public void write(DataOutput out) throws IOException {
    WritableUtils.writeString(out, indexType.toString());
    if (IndexType.isUserDefinedIndex(indexType)) {
      WritableUtils.writeVInt(out, indexRelations.size());
      // write index relations
      for (IndexRelationship r : indexRelations) {
        r.write(out);
      }
    } else {
      // write index family map
      writeFamilyMap(indexFamilyMap, out);
      writeFamilyMap(familyMap, out);
    }
  }

  private void writeFamilyMap(TreeMap<byte[], TreeSet<byte[]>> map, DataOutput out)
      throws IOException {
    WritableUtils.writeVInt(out, map.size());
    for (Map.Entry<byte[], TreeSet<byte[]>> entry : map.entrySet()) {
      WritableUtils.writeCompressedByteArray(out, entry.getKey());
      WritableUtils.writeVInt(out, entry.getValue().size());
      for (byte[] b : entry.getValue()) {
        WritableUtils.writeCompressedByteArray(out, b);
      }
    }
  }

  /**
   * read fields, not only index attributes are included
   *
   * @param in
   * @throws IOException
   */
  @Override public void readFields(DataInput in) throws IOException {
    indexType = IndexType.valueOf(WritableUtils.readString(in));
    if (IndexType.isUserDefinedIndex(indexType)) {
      int size = WritableUtils.readVInt(in);
      indexRelations = new ArrayList<>(size);
      for (int i = 0; i < size; ++i) {
        IndexRelationship relationship = new IndexRelationship();
        relationship.readFields(in);
        indexRelations.add(relationship);
      }
    } else {
      indexFamilyMap = readTreeMap(in);
      familyMap = readTreeMap(in);
    }
  }

  private TreeMap<byte[], TreeSet<byte[]>> readTreeMap(DataInput in) throws IOException {
    TreeMap<byte[], TreeSet<byte[]>> map = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    int size = WritableUtils.readVInt(in);
    for (int i = 0; i < size; i++) {
      byte[] key = WritableUtils.readCompressedByteArray(in);
      int vSize = WritableUtils.readVInt(in);
      TreeSet<byte[]> set = new TreeSet<>(Bytes.BYTES_COMPARATOR);
      for (int j = 0; j < vSize; j++) {
        set.add(WritableUtils.readCompressedByteArray(in));
      }
      map.put(key, set);
    }
    return map;
  }

  @Override public boolean equals(Object o) {
    if (o == null || !(o instanceof IndexTableRelation)) return false;
    IndexTableRelation o2 = (IndexTableRelation) o;
    try {
      if (tableName.equals(o2.tableName)) {
        if (Bytes.compareTo(getIndexAttribute(), o2.getIndexAttribute()) == 0) {
          return true;
        }
      }
    } catch (IOException e) {
    }
    return false;
  }

  public TreeMap<byte[], TreeSet<byte[]>> getFamilyMap() {
    return familyMap;
  }

  public TreeMap<byte[], TreeSet<byte[]>> getIndexFamilyMap() {
    return indexFamilyMap;
  }

  public Map<ColumnInfo, Set<ColumnInfo>> getIndexColumnMap() {
    if (indexType == IndexType.NoIndex || IndexType.isSecondaryIndex(indexType)) return null;
    if (indexColumnMap != null) return indexColumnMap;
    // gather all index columns
    List<ColumnInfo> allIndexColumns = new ArrayList<>();
    for (Map.Entry<byte[], TreeSet<byte[]>> entry : getIndexFamilyMap().entrySet()) {
      for (byte[] q : entry.getValue()) {
        allIndexColumns.add(new ColumnInfo(entry.getKey(), q));
      }
    }
    indexColumnMap = new HashMap<>();
    if (IndexType.isClusteringIndex(indexType)) {
      // for clustering index, each column us needed by the rest columns
      for (Map.Entry<byte[], TreeSet<byte[]>> entry : getFamilyMap().entrySet()) {
        for (byte[] q : entry.getValue()) {
          ColumnInfo curCol = new ColumnInfo(entry.getKey(), q);
          curCol.setIsIndex(allIndexColumns.contains(curCol));
          Set<ColumnInfo> copy = new HashSet<>(allIndexColumns);
          copy.remove(curCol);
          indexColumnMap.put(curCol, copy);
        }
      }
    } else {
      // must be user defined index, check additional columns one by one
      for (IndexRelationship relation : indexRelations) {
        for (ColumnInfo dest : relation.getAdditionalColumns()) {
          Set<ColumnInfo> srcs = indexColumnMap.get(dest);
          if (srcs == null) {
            srcs = new HashSet<>();
            indexColumnMap.put(dest, srcs);
          }
          srcs.add(relation.getMainColumn());
        }
      }
    }
    return indexColumnMap;
  }

  public boolean isIndexColumn(byte[] f, byte[] q) {
    if (indexType == IndexType.NoIndex) return false;
    if (IndexType.isUserDefinedIndex(indexType)) {
      for (IndexRelationship relationship : indexRelations) {
        if (relationship.getMainColumn().isSameColumn(f, q)) return true;
      }
      return false;
    } else {
      TreeSet<byte[]> set = indexFamilyMap.get(f);
      if (set == null) return false;
      return set.contains(q);
    }
  }

  public boolean isIndexColumn(ColumnInfo info) {
    return isIndexColumn(info.getFamily(), info.getQualifier());
  }

  /**
   * get table name for given family and qualifier, with indexType
   *
   * @param family
   * @param qualifier
   * @return
   */
  public TableName getIndexTableName(byte[] family, byte[] qualifier) {
    return TableName.valueOf(Bytes.add(Bytes.add(tableName.getName(), TABLE_SEPARATOR),
        Bytes.add(family, TABLE_SEPARATOR, qualifier),
        Bytes.add(TABLE_SEPARATOR, Bytes.toBytes(indexType.toString()))));
  }

  public TableName getIndexTableName(ColumnInfo info) {
    return getIndexTableName(info.getFamily(), info.getQualifier());
  }

  public TableName getCCTName(byte[] family, byte[] qualifier) {
    return TableName.valueOf(Bytes.add(Bytes.add(tableName.getName(), TABLE_SEPARATOR),
        Bytes.add(family, TABLE_SEPARATOR, qualifier),
        Bytes.add(TABLE_SEPARATOR, IndexType.CCT_SUFFIX_BYTES)));
  }

  public TableName getCCTName(ColumnInfo info) {
    return getCCTName(info.getFamily(), info.getQualifier());
  }

  public List<IndexRelationship> getIndexRelations() {
    return indexRelations;
  }

  /**
   * set user-defined index relationships, only available for tables with user-defined index
   *
   * @param list
   * @throws IllegalIndexException
   */
  public void setIndexRelations(List<IndexRelationship> list) throws IllegalIndexException {
    if (!IndexType.isUserDefinedIndex(indexType)) {
      throw new IllegalIndexException(tableName, indexType,
          "should not has user-defined index table");
    }
    if (list == null || list.isEmpty()) indexRelations = null;
    else {
      indexRelations = list;
      // check relations
      for (IndexRelationship one : list) {
        indexFamilyMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
        byte[] family = one.getMainColumn().getFamily();
        TreeSet<byte[]> set = indexFamilyMap.get(family);
        if (set == null) {
          set = new TreeSet<>(Bytes.BYTES_COMPARATOR);
          indexFamilyMap.put(family, set);
        }
        if (set.contains(one.getMainColumn().getQualifier()))
          throw new IllegalIndexException(tableName, indexType,
              " should not contain different index relations on the same column: " + ColumnInfo
                  .transToString(family, one.getMainColumn().getQualifier()));
        set.add(one.getMainColumn().getQualifier());
      }
    }
  }

  public static HColumnDescriptor getDefaultColumnDescriptor(byte[] family) {
    HColumnDescriptor colDesc = new HColumnDescriptor(family);
    //    colDesc.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
    colDesc.setDataBlockEncoding(DataBlockEncoding.NONE);
    colDesc.setCompressionType(Compression.Algorithm.NONE);
    return colDesc;
  }

  public static HColumnDescriptor getDefaultColumnDescriptor(String family) {
    return getDefaultColumnDescriptor(Bytes.toBytes(family));
  }
}
