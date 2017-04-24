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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author shoji
 */
public class MDRangeFilter extends FilterBase implements Writable {

  public MDRangeFilter() {

  }

  private MDRange[] ranges;

  public MDRangeFilter(MDRange[] ranges) {
    this.ranges = ranges;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
   */
  @Override public void readFields(DataInput in) throws IOException {
    int size = in.readInt();
    ranges = new MDRange[size];
    for (int i = 0; i < size; i++) {
      int min = in.readInt();
      int max = in.readInt();
      ranges[i] = new MDRange(min, max);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
   */
  @Override public void write(DataOutput out) throws IOException {
    out.writeInt(ranges.length);
    for (MDRange r : ranges) {
      out.writeInt(r.min);
      out.writeInt(r.max);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hbase.filter.FilterBase#filterKeyValue(org.apache.hadoop
   * .hbase.KeyValue)
   */
  @Override public ReturnCode filterKeyValue(Cell cell) {
    byte[] value = cell.getValue();
    for (int i = 0; i < ranges.length; i++) {
      int v = Bytes.toInt(value, i * 4);
      if (!ranges[i].include(v)) {
        return ReturnCode.NEXT_ROW;
      }
    }
    return ReturnCode.INCLUDE;
  }
}
