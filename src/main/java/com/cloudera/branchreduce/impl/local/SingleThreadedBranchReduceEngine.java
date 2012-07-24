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
package com.cloudera.branchreduce.impl.local;

import org.apache.hadoop.io.Writable;

import com.cloudera.branchreduce.BranchReduceContext;
import com.cloudera.branchreduce.BranchReduceEngine;
import com.cloudera.branchreduce.BranchReduceJob;
import com.cloudera.branchreduce.GlobalState;
import com.cloudera.branchreduce.Processor;

/**
 * A local, single-threaded implementation of the {@code BranchReduceEngine} strategy.
 */
public class SingleThreadedBranchReduceEngine implements BranchReduceEngine {
  public <T extends Writable, G extends GlobalState<G>> BranchReduceContext<T, G> run(
      BranchReduceJob<T, G> job) {
    final Processor<T, G> processor = job.constructProcessor();
    final BranchReduceContext<T, G> ctxt = new BranchReduceContext<T, G>(
        job.getConfiguration(), job.constructGlobalState());
    processor.initialize(ctxt);
    processor.execute(job.constructInitialTask(), ctxt);
    while (!ctxt.isTaskQueueEmpty()) {
      processor.execute(ctxt.take(), ctxt);
    }
    processor.cleanup(ctxt);
    return ctxt;
  }
}
