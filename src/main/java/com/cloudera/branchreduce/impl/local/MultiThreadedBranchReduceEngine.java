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

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

import com.cloudera.branchreduce.BranchReduceContext;
import com.cloudera.branchreduce.BranchReduceEngine;
import com.cloudera.branchreduce.BranchReduceJob;
import com.cloudera.branchreduce.GlobalState;
import com.cloudera.branchreduce.Processor;
import com.google.common.collect.Lists;

/**
 * An implementation of a {@code BranchReduceEngine} that starts multiple threads via an
 * {@code ExecutorService} in order to execute tasks.
 */
public class MultiThreadedBranchReduceEngine implements BranchReduceEngine {

  private final ExecutorService executor;
  
  public MultiThreadedBranchReduceEngine(int numThreads) {
    this(Executors.newFixedThreadPool(numThreads));
  }
  
  public MultiThreadedBranchReduceEngine(ExecutorService executor) {
    this.executor = executor;
  }
  
  @Override
  public <T extends Writable, G extends GlobalState<G>> BranchReduceContext<T, G> run(
      BranchReduceJob<T, G> job) {
    final Processor<T, G> processor = job.constructProcessor();
    final Context<T, G> ctxt = new Context<T, G>(job.getConfiguration(), job.constructGlobalState());
    processor.initialize(ctxt);
    
    // Get the initial set of tasks.
    processor.execute(job.constructInitialTask(), ctxt);
    
    List<Future<?>> inFlight = Lists.newLinkedList();
    do {
      // Start more tasks when there is work to do.
      while (!ctxt.isTaskQueueEmpty()) {
        inFlight.add(executor.submit(new Runnable() {
          @Override
          public void run() {
            processor.execute(ctxt.take(), ctxt);
          } 
        }));
      }
      
      // Wait a second.
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        
      }
      
      // Check on everyone's status.
      Iterator<Future<?>> iter = inFlight.iterator();
      while (iter.hasNext()) {
        Future<?> f = iter.next();
        if (f.isDone()) {
          iter.remove();
        }
      }
      
    } while (!inFlight.isEmpty() && !ctxt.isTaskQueueEmpty());
    
    processor.cleanup(ctxt);
    return ctxt;
  }

  public class Context<T extends Writable, G extends GlobalState<G>> extends BranchReduceContext<T, G> {
    public Context(Configuration conf, G globalState) {
      super(conf, globalState);
    }

    @Override
    public synchronized boolean updateGlobalState(G other) {
      return readGlobalState().mergeWith(other);
    }
  }
}
