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
package com.cloudera.branchreduce;

import org.apache.hadoop.io.Writable;

/**
 * Processes an input task and 1) emits zero or more child tasks and 2) potentially
 * updates the {@code GlobalState} associated with a {@code BranchReduceJob}.
 *
 * @param <T> The task type that this processor operates on.
 * @param <G> The {@code GlobalState} type used by this processor.
 */
public abstract class Processor<T extends Writable, G extends GlobalState<G>> {
  
  public void initialize(BranchReduceContext<T, G> context) { }
  
  abstract public void execute(T task, BranchReduceContext<T, G> context);
  
  public void cleanup(BranchReduceContext<T, G> context) { }
}
