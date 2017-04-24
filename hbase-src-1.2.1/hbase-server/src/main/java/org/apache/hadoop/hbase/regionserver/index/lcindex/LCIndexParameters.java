package org.apache.hadoop.hbase.regionserver.index.lcindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.index.IndexType;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;

/**
 * Created by winter on 16-12-6.
 */
public class LCIndexParameters {

  private final Configuration conf;
  private final IndexType indexType;
  // hdfsRegionDir = hdfs://localhost:9000/hbase/data/default/[tableName]/[regionId]
  private final Path hdfsRegionDir;
  private final String tableName;
  private final String regionId;
  private final Path localRoot; // set by configuration
  private final Path localRegionDir; // [localRoot]/[tableName]/[regionId]
  private Path localTmpDir; // [localRoot]/[tableName]/[regionId]/.tmp
  public ArrayList<String> lcRegionServerHostnames = null;
  private String lcPrevHoldServer = null;
  //  private LocalFileSystem localFS = null;
  private FileSystem fs = null;
  private Path lcIndexLocalBaseDir;
  private InetAddress localIP = null;

  public LCIndexParameters(FileSystem defaultFS, Configuration conf, IndexType indexType,
      Path hdfsRegionDir) throws IOException {
    this.conf = conf;
    this.indexType = indexType;
    localIP = InetAddress.getLocalHost();
    this.hdfsRegionDir = hdfsRegionDir;
    regionId = hdfsRegionDir.getName();
    tableName = hdfsRegionDir.getParent().getName();
    String localRootStr = conf.get(LCIndexConstant.LCINDEX_LOCAL_DIR);
    if (localRootStr == null) throw new RuntimeException(
        "set " + LCIndexConstant.LCINDEX_LOCAL_DIR + " when using LCIndex");
    // System.out.println("hdfs region dir: " + hdfsRegionDir);
    localRoot = new Path(localRootStr);
    localRegionDir = new Path(new Path(localRoot, tableName), regionId);
    localTmpDir = new Path(localRegionDir, LCIndexConstant.LCINDEX_TMP_DIR_NAME);
    boolean useLocal = conf.getBoolean(LCIndexConstant.LCINDEX_USE_LOCAL_FS, true);
    if (useLocal) {
      fs = LocalFileSystem.getLocal(conf);
    } else {
      fs = defaultFS;
    }
    if (fs == null) {
      throw new IOException("localFS is null in LCIndex");
    }
    String hosts = conf.get(LCIndexConstant.LCINDEX_REGIONSERVER_HOSTNAMES);
    if (hosts == null) {
      throw new IOException("lcindex miss RegionServer hosts, assign "
          + LCIndexConstant.LCINDEX_REGIONSERVER_HOSTNAMES + " at first, multi hosts with "
          + LCIndexConstant.LCINDEX_REGIONSERVER_HOSTNAMES_DELIMITER);
    }
    String parts[] = hosts.split(LCIndexConstant.LCINDEX_REGIONSERVER_HOSTNAMES_DELIMITER);
    lcRegionServerHostnames = new ArrayList<>();
    for (String hostname : parts) {
      lcRegionServerHostnames.add(hostname);
    }
  }

  public static Path getLocalBaseDirByFileName(Path tempHome, Path rawHFile) {
    return new Path(tempHome, rawHFile.getName() + LCIndexConstant.LCINDEX_DIR_NAME);
  }

  //  public Path getLocalBaseDirByFileName(Path rawHFile) {
  //    return getLocalBaseDirByFileName(localTmpDir, rawHFile);
  //  }

  //  public Path getLCLocalHome() {
  //    return localRoot;
  //  }

  public FileSystem getLCIndexFileSystem() {
    return fs;
  }

  /**
   * LC_Home/[tableName]/[regionId]/.tmp/[HFileId].lcindex
   */
  public Path getTmpDirPath(Path hdfsTmpPath) {
    return new Path(localTmpDir, hdfsTmpPath.getName() + LCIndexConstant.LCINDEX_DIR_NAME);
  }

  /**
   * LC_Home/[tableName]/[regionId]/.tmp/[HFileId].lcindex/[qualifier]
   */
  public Path getTmpIFilePath(Path hdfsTmpPath, byte[] qualifier) {
    return new Path(getTmpDirPath(hdfsTmpPath), Bytes.toString(qualifier));
  }

  /**
   * LC_Home/[tableName]/[regionId]/.tmp/[HFileId].lcindex/[qualifier]-stat
   */
  public Path getTmpSFilePath(Path hdfsTmpPath, byte[] qualifier) {
    return new Path(getTmpDirPath(hdfsTmpPath),
        Bytes.toString(qualifier) + LCIndexConstant.LC_STAT_FILE_SUFFIX);
  }

  /**
   * LC_Home/[tableName]/[regionId]/[family]/[HFileId].lcindex
   * hdfsPath = /hdfs-hbase/[something...]/[tableName]/[regionId]/[family]/[HFileId]
   */
  public Path getLocalDir(Path hdfsPath) {
    String hfileId = hdfsPath.getName();
    String family = hdfsPath.getParent().getName();
    return new Path(new Path(localRegionDir, family), hfileId + LCIndexConstant.LCINDEX_DIR_NAME);
  }

  /**
   * LC_Home/[tableName]/[regionId]/[family]/[HFileId].lcindex/[qualifier]-stat
   * hdfsPath = /hdfs-hbase/[something...]/[tableName]/[regionId]/[family]/[HFileId]
   */
  public Path getLocalSFile(Path hdfsPath, byte[] qualifier) {
    return new Path(getLocalDir(hdfsPath),
        Bytes.toString(qualifier) + LCIndexConstant.LC_STAT_FILE_SUFFIX);
  }

  /**
   * LC_Home/[tableName]/[regionId]/[family]/[HFileId].lcindex/[qualifier]
   * hdfsPath = /hdfs-hbase/[something...]/[tableName]/[regionId]/[family]/[HFileId]
   */
  public Path getLocalIFile(Path hdfsPath, byte[] qualifier) {
    return new Path(getLocalDir(hdfsPath), Bytes.toString(qualifier));
  }

  public static Path iFileToSFile(Path iFile) {
    return new Path(iFile.getParent(), iFile.getName() + LCIndexConstant.LC_STAT_FILE_SUFFIX);
  }
}
