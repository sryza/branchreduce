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

import java.io.File;
import java.io.IOException;
import java.util.BitSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;

import com.cloudera.branchreduce.BranchReduceContext;
import com.cloudera.branchreduce.BranchReduceJob;
import com.cloudera.branchreduce.Processor;
import com.cloudera.branchreduce.globalstate.MinimumInt;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

/**
 * TODO
 */
public class ImplicitEnumerationSolver extends Processor<PartialSolution, CurrentBestSolution> {

  public static final String LP_PROBLEM = "branchreduce.lp.problem";
  
  public static BranchReduceJob<PartialSolution, CurrentBestSolution> createJob(
      boolean runLocally, File lpFile) {
    return createJob(runLocally, lpFile, new Configuration());
  }
  
  public static BranchReduceJob<PartialSolution, CurrentBestSolution> createJob(
      boolean runLocally, File lpFile, Configuration conf) {
    BranchReduceJob<PartialSolution, CurrentBestSolution> job =
        new BranchReduceJob<PartialSolution, CurrentBestSolution>(runLocally, conf);
    
    job.setProcessorClass(ImplicitEnumerationSolver.class);
    job.setTaskClass(PartialSolution.class);
    job.setGlobalStateClass(CurrentBestSolution.class);
    job.setJarByClass(ImplicitEnumerationSolver.class);
    
    try {
      String problem = Joiner.on("\n").join(Files.readLines(lpFile, Charsets.UTF_8));
      job.getConfiguration().set(LP_PROBLEM, problem);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    
    return job;
  }
  
  private Expression objective;
  private BitSet coefSigns;
  private int numVariables;
  private List<Constraint> constraints;
  
  @Override
  public void initialize(BranchReduceContext<PartialSolution, CurrentBestSolution> context) {
    if (objective == null) {
      Configuration conf = context.getConfiguration();
      String lpProblem = conf.get(LP_PROBLEM, "");
      if (lpProblem.isEmpty()) {
        throw new IllegalArgumentException("No branchreduce.lp.problem config value specified, exiting");
      }
      SimplifiedLpParser parser = new SimplifiedLpParser(lpProblem);
      parser.parse();
      init(parser.getObjective(), parser.getConstraints());
    }
  }

  private void init(Expression objective, List<Constraint> constraints) {
    this.objective = objective;
    this.numVariables = objective.getNumVariables();
    this.constraints = constraints;
    this.coefSigns = objective.coefSigns();
    this.coefSigns.flip(0, numVariables);
  }
  
  
  @Override
  public void execute(PartialSolution solution, BranchReduceContext<PartialSolution, CurrentBestSolution> context) {
    // First, see if the completion is feasible.
    int fixLimit = solution.getFixLimit();
    BitSet completion = fixLimit < numVariables ? coefSigns.get(fixLimit, numVariables) : new BitSet();
    boolean feasible = true;
    int[] deltas = new int[constraints.size()];
    for (int i = 0; i < deltas.length; i++) {
      deltas[i] = constraints.get(i).delta(solution, completion);
      if (deltas[i] < 0) {
        feasible = false;
      }
    }

    int bestPossible = objective.eval(solution, completion);
    int currentBestValue = context.readGlobalState().getValue();
    if (feasible) {
      if (bestPossible < currentBestValue) {
        // Awesome, the best possible solution is feasible. We've fathomed the problem.
        context.updateGlobalState(new CurrentBestSolution(
            solution.complete(completion, numVariables), new MinimumInt(bestPossible)));
      }
      return;
    }
    
    // Okay, so maybe that didn't work. What's next? Well, we should check to see if _any_
    // feasible solution would be better than the current best solution.
    int[] dlimit = new int[deltas.length];
    for (int i = fixLimit; i < numVariables; i++) {
      boolean completes = completion.get(i - fixLimit);
      int baseValue = bestPossible + (completes ? -objective.getCoef(i) : objective.getCoef(i));
      if (baseValue < currentBestValue) {
        // Okay, so it can improve the objective.
        for (int j = 0; j < deltas.length; j++) {
          if (deltas[j] < 0) {
            int c = constraints.get(j).getCoef(i);
            if ((c < 0 && !completes) || (c > 0 && completes)) {
              dlimit[j] += Math.abs(c);
            }
          }
        }
      }
    }
    
    for (int j = 0; j < deltas.length; j++) {
      if (dlimit[j] + deltas[j] < 0) {
        // Can't possibly satisfy the constraints-- fathom it.
        return;
      }
    }
    
    // Otherwise, keep going down the tree.
    context.emit(solution.getNext(false), solution.getNext(true));
  }
}
