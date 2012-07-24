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
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

/**
 *
 */
public class Expression {

  private final int[] coef;
  
  public static Expression of(int... values) {
    return new Expression(values);
  }
  
  public Expression(int[] coef) {
    this.coef = coef;
  }
  
  public BitSet coefSigns() {
    BitSet signs = new BitSet(coef.length);
    for (int i = 0; i < coef.length; i++) {
      if (coef[i] >= 0) {
        signs.set(i);
      }
    }
    return signs;
  }
  
  public int getNumVariables() {
    return coef.length;
  }
  
  public int getCoef(int index) {
    return coef[index];
  }
  
  public int eval(PartialSolution sol) { 
    int sum = 0;
    int fixedLimit = sol.getFixLimit();
    for (int i = 0; i < fixedLimit; i++) {
      if (coef[i] != 0) {
        sum += coef[i] * sol.get(i);
      }
    }
    return sum;
  }
  
  public int eval(PartialSolution sol, BitSet completion) {
    int sum = eval(sol);
    int fixedLimit = sol.getFixLimit();
    for (int i = 0; i < coef.length - fixedLimit; i++) {
      if (completion.get(i)) {
        sum += coef[fixedLimit + i];
      }
    }
    return sum;
  }
  
  @Override
  public String toString() {
    List<String> terms = Lists.newArrayList();
    for (int i = 0; i < coef.length; i++) {
      if (coef[i] != 0.0) {
        terms.add(String.format("%dx%d", coef[i], i + 1));
      }
    }
    return Joiner.on(" + ").join(terms);
  }
}
