/*
 * Copyright 2012 Shoji Nishimura
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.index.mdhbase;

import org.apache.hadoop.hbase.util.Bytes;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author shoji
 */
public class MDPoint {
  public final byte[] id;
  public final int[] values;

  public MDPoint(byte[] id, int[] values) {
    for (int v : values) {
      checkArgument(0 <= v);
    }
    this.id = id;
    this.values = values;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  @Override public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[").append(id).append(", (");
    for (int v : values) {
      sb.append(v).append(",");
    }
    sb.append(")]");
    return sb.toString();
  }

  public byte[] toQualifier() {
    return id;
  }

  public byte[] toValue() {
    byte[][] bytesArray = new byte[values.length][];
    for (int i = 0; i < values.length; i++) {
      bytesArray[i] = Bytes.toBytes(values[i]);
    }
    return MDUtils.concat(bytesArray);
  }
}
