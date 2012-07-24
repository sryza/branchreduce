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

import java.util.BitSet;

/**
 * Expresses a less-than-or-equal-to constraint between an expression and a constant.
 */
public class Constraint {
  private final Expression expr;
  private final int value;
  
  public Constraint(Expression expr, int value) {
    this.expr = expr;
    this.value = value;
  }
  
  public int getCoef(int index) {
    return expr.getCoef(index);
  }
  
  public int delta(PartialSolution solution, BitSet completion) {
    return value - expr.eval(solution, completion);
  }
  
  public boolean satisfied(PartialSolution solution, BitSet completion) {
    return 0 <= delta(solution, completion);
  }
  
  @Override
  public String toString() {
    return new StringBuilder(expr.toString()).append(" <= ").append(value).toString();
  }
}
