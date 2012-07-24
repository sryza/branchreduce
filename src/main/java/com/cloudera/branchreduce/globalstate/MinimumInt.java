/**
 * Copyright (c) 2012, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.branchreduce.globalstate;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.cloudera.branchreduce.GlobalState;

/**
 * A {@code GlobalState} implementation that keeps track of a minimal integer value.
 */
public class MinimumInt implements GlobalState<MinimumInt> {

  private int value;
  
  public MinimumInt() {
    this(Integer.MAX_VALUE);
  }

  public MinimumInt(int value) {
    this.value = value;
  }
  
  public int getValue() {
    return value;
  }
  
  public boolean maybeSetValue(int other) {
    if (other < value) {
      this.value = other;
      return true;
    }
    return false;
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    this.value = in.readInt();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(value);
  }

  @Override
  public boolean mergeWith(MinimumInt other) {
    return maybeSetValue(other.value);
  }
}
