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

import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author shoji
 */
public class MDUtils {
  private MDUtils() {

  }

  public static byte[] bitwiseZip(int[] arr, int dimensions) {
    checkArgument(arr.length == dimensions);
    int fixedSize = arr.length * 4;
    byte[] ret = new byte[fixedSize];
    int len = arr.length;
    for (int i = 0; i < 32; i++) {
      for (int j = 0; j < len; j++) {
        if (((arr[j] >>> i) & 1) == 0) continue;
        int bitPos = i * len + (len - j - 1);
        int index = ret.length - 1 - bitPos / 8;
        int offset = bitPos % 8;
        ret[index] |= (1 << offset);
      }
    }
    return ret;
  }

  private static final int[] MASKS =
      new int[] { 0xFFFF0000, 0xFF00FF00, 0xF0F0F0F0, 0xCCCCCCCC, 0xAAAAAAAA };

  //  public static int makeGap(int x) {
  //    int x0 = x & MASKS[0];
  //    int x1 = (x0 | (x0 >>> 8)) & MASKS[1];
  //    int x2 = (x1 | (x1 >>> 4)) & MASKS[2];
  //    int x3 = (x2 | (x2 >>> 2)) & MASKS[3];
  //    int x4 = (x3 | (x3 >>> 1)) & MASKS[4];
  //    return x4;
  //  }

  public static int[] bitwiseUnzip(byte[] bs, int dimensions) {
    int fixedSize = dimensions * 4;
    int intLen = bs.length / 4;
    int[] ret = new int[intLen];
    Arrays.fill(ret, 0);
    for (int i = 0; i < 32; i++) {
      for (int j = 0; j < intLen; j++) {
        int bitPos = i * intLen + (intLen - j - 1);
        int index = bs.length - 1 - bitPos / 8;
        int offset = bitPos % 8;
        if ((bs[index] & (1 << offset)) == 0) continue;
        ret[j] |= (1 << i);
      }
    }
    return ret;
  }

  //  public static int elimGap(int x) {
  //    int x0 = x & MASKS[4];
  //    int x1 = (x0 | (x0 << 1)) & MASKS[3];
  //    int x2 = (x1 | (x1 << 2)) & MASKS[2];
  //    int x3 = (x2 | (x2 << 4)) & MASKS[1];
  //    int x4 = (x3 | (x3 << 8)) & MASKS[0];
  //    return x4;
  //  }

  public static byte[] concat(byte[][] bytesArray) {
    checkNotNull(bytesArray);
    int len = 0;
    for (byte[] bytes : bytesArray) {
      checkNotNull(bytes);
      len += bytes.length;
    }
    byte[] ret = new byte[len];
    int offset = 0;
    for (byte[] bytes : bytesArray) {
      System.arraycopy(bytes, 0, ret, offset, bytes.length);
      offset += bytes.length;
    }
    return ret;
  }

  //  public static boolean prefixMatch(byte[] prefix, int prefixSize, byte[] target) {
  //    byte[] mask = makeMask(prefixSize);
  //    checkArgument(prefix.length >= mask.length);
  //    checkArgument(target.length >= mask.length);
  //    for (int i = 0; i < mask.length; i++) {
  //      if (!(prefix[i] == (target[i] & mask[i]))) {
  //        return false;
  //      }
  //    }
  //    return true;
  //  }

  public static byte[] makeMask(int prefixSize, int dimensions) {
    checkArgument(prefixSize > 0);
    int d = (prefixSize - 1) / 8;
    int r = (prefixSize - 1) % 8;
    // mask[] = {0b10000000, 0b11000000, ..., 0b11111111}
    final byte[] mask = new byte[] { -128, -64, -32, -16, -8, -4, -2, -1 };
    byte[] ret = new byte[dimensions * 4];
    for (int i = 0; i < d; i++) {
      ret[i] = -1; // 0xFF
    }
    ret[d] = mask[r];
    return ret;
  }

  public static byte[] not(byte[] bs) {
    byte[] ret = new byte[bs.length];
    for (int i = 0; i < bs.length; i++) {
      ret[i] = (byte) (~bs[i]);
    }
    return ret;
  }

  public static byte[] or(byte[] b1, byte[] b2) {
    checkArgument(b1.length == b2.length);
    byte[] ret = new byte[b1.length];
    for (int i = 0; i < b1.length; i++) {
      ret[i] = (byte) (b1[i] | b2[i]);
    }
    return ret;
  }

  public static byte[] and(byte[] b1, byte[] b2) {
    checkArgument(b1.length == b2.length);
    byte[] ret = new byte[b1.length];
    for (int i = 0; i < b1.length; i++) {
      ret[i] = (byte) (b1[i] & b2[i]);
    }
    return ret;
  }

  public static byte[] makeBit(byte[] target, int pos, int dimensions) {
    checkArgument(pos >= 0);
    checkArgument(pos < target.length * 4 * dimensions);
    int d = pos / 8;
    int r = pos % 8;
    final byte[] bits = new byte[] { -128, 64, 32, 16, 8, 4, 2, 1 };
    byte[] ret = new byte[target.length];
    System.arraycopy(target, 0, ret, 0, target.length);
    ret[d] |= bits[r]; // same as " ret[d] |= (1 << r);"
    return ret;
  }

  public static String toString(byte[] key, int prefixLength) {
    StringBuilder buf = new StringBuilder();
    int d = (prefixLength - 1) / 8;
    int r = (prefixLength - 1) % 8;
    final int[] masks = new int[] { -128, 64, 32, 16, 8, 4, 2, 1 };
    for (int i = 0; i < d; i++) {
      for (int j = 0; j < masks.length; j++) {
        buf.append((key[i] & masks[j]) == 0 ? "0" : "1");
      }
    }
    for (int j = 0; j <= r; j++) {
      buf.append((key[d] & masks[j]) == 0 ? "0" : "1");
    }
    for (int j = r + 1; j < masks.length; j++) {
      buf.append("*");
    }
    for (int i = d + 1; i < key.length; i++) {
      buf.append("********");
    }
    return buf.toString();
  }

  public static byte[] increment(byte[] bytes) {
    for (int i = bytes.length - 1; i >= 0; i++) {
      if (bytes[i] != 0xff) {
        ++bytes[i];
        break;
      }
      ++bytes[i];
    }
    return bytes;
  }

  public static int getSuffix(int b) {
    for (int i = 31; i >= 0; i--) {
      if (((b >> i) & 1) > 0) {
        return i;
      }
    }
    return 0;
  }

  public static int getPrefix(byte[] b) {
    for (int i = 0; i < b.length; i++) {
      for (int j = 7; j >= 0; j--) {
        if (((b[i] >> j) & 1) > 0) {
          return i * 8 + 8 - j;
        }
      }
    }
    return b.length * 8;
  }

  public static void maskSuffix(byte[] bytes, int suffix) {
    final byte[] mask = new byte[] { -1, -2, -4, -8, -16, -32, -64, -128, 0 };
    int index = suffix / 8;
    int offset = suffix % 8;
    bytes[bytes.length - 1 - index] &= mask[offset];
    for (int i = bytes.length - index; i < bytes.length; i++) {
      bytes[i] = 0;
    }
  }

  /**
   * split the bytes, as far as target length
   * in a int, 100 (4) prefix=30, calling 4, 31, 31, generates 100(4) and 110(6), splits into two
   */
  public static void splitBytes(List<byte[]> list, byte[] bytes, int targetLen, int curPos) {
    if (curPos > targetLen) {
      list.add(bytes);
      return;
    }
    // zeros
    splitBytes(list, bytes, targetLen, curPos + 1);
    // ones
    byte[] one = new byte[bytes.length];
    System.arraycopy(bytes, 0, one, 0, bytes.length);
    int i = curPos / 8;
    int offset = curPos % 8;
    if (offset == 0) {
      offset = 8;
      i--;
    }
    one[i] = (byte) (one[i] | (1 << (8 - offset)));
    splitBytes(list, one, targetLen, curPos + 1);
  }

  public static String toBitString(byte[] bytes) {
    StringBuilder sb = new StringBuilder();
    boolean skip = true;
    for (byte b : bytes) {
      if (b == 0 && skip) {
      } else {
        skip = false;
        for (int i = 7; i >= 0; i--) {
          sb.append( (b >> i) & 1);
        }
      }
    }
    return sb.toString();
  }
}
