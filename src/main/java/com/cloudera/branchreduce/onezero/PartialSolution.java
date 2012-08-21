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
package com.cloudera.branchreduce.onezero;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.BitSet;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 *
 */
public class PartialSolution implements Writable {

  private int fixLimit;
  private int numVariables;
  private BitSet values;
  
  public PartialSolution() {
    this(0);
  }
  
  public PartialSolution(int numVariables) {
    this(0, numVariables, new BitSet());
  }

  public PartialSolution(int fixLimit, int numVariables, BitSet values) {
    this.fixLimit = fixLimit;
    this.numVariables = numVariables;
    this.values = values;
  }
  
  public int getNumVariables() {
    return values.size();
  }
  
  public int getFixLimit() {
    return fixLimit;
  }
  
  public boolean isComplete() {
    return fixLimit == -1;
  }
  
  public PartialSolution getNext(boolean on) {
    BitSet v = new BitSet(fixLimit + 1);
    v.or(values);
    v.set(fixLimit, on);
    return new PartialSolution(fixLimit + 1, numVariables, v);
  }
  
  public PartialSolution complete(BitSet completion, int numVariables) {
    BitSet v = new BitSet(values.size());
    v.or(values);
    for (int i = 0; i < numVariables - fixLimit; i++) {
      v.set(i + fixLimit, completion.get(i));
    }
    return new PartialSolution(-1, numVariables, v);
  }
  
  public int get(int index) {
    if (fixLimit > -1 && index < fixLimit) {
      return values.get(index) ? 1 : 0;
    }
    throw new IllegalArgumentException(
        "Index " + index + " exceeds the fixed limit of " + fixLimit);
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    this.numVariables = WritableUtils.readVInt(in);
    this.fixLimit = WritableUtils.readVInt(in);
    this.values.clear();
    for (int i = 0; i < Math.max(numVariables, fixLimit); i++) {
      values.set(i, in.readBoolean());
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, numVariables);
    WritableUtils.writeVInt(out, fixLimit);
    for (int i = 0; i < Math.max(numVariables, fixLimit); i++) {
      out.writeBoolean(values.get(i));
    }
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    if (isComplete()) {
      sb.append("Complete Solution = ");
    } else {
      sb.append("Partial(" + fixLimit + ") Solution = ");
    }
    sb.append(values);
    return sb.toString();
  }
}
