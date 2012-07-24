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

import com.cloudera.branchreduce.GlobalState;
import com.cloudera.branchreduce.globalstate.MinimumInt;

/**
 *
 */
public class CurrentBestSolution implements GlobalState<CurrentBestSolution> {

  private PartialSolution solution;
  private MinimumInt value;
  
  public CurrentBestSolution() {
    this(new PartialSolution(), new MinimumInt());
  }
  
  public CurrentBestSolution(PartialSolution solution, MinimumInt value) {
    this.solution = solution;
    this.value = value;
  }
  
  public int getValue() {
    return value.getValue();
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    this.solution.readFields(in);
    this.value.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    this.solution.write(out);
    this.value.write(out);
  }

  @Override
  public boolean mergeWith(CurrentBestSolution other) {
    if (value.mergeWith(other.value)) {
      this.solution = other.solution;
      return true;
    }
    return false;
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("Objective Value = ");
    sb.append(value.getValue()).append("\n");
    sb.append(solution.toString());
    return sb.toString();
  }
}
