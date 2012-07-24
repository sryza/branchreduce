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
package com.cloudera.branchreduce.impl.distributed;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

import com.cloudera.branchreduce.BranchReduceContext;
import com.cloudera.branchreduce.BranchReduceJob;
import com.cloudera.branchreduce.GlobalState;
import com.cloudera.branchreduce.Processor;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractExecutionThreadService;

public class Worker<T extends Writable, G extends GlobalState<G>> extends AbstractExecutionThreadService {

  public interface TaskMasterProxy<T, G> {
    int registerWorker(String hostname, int port);
    
    List<T> getWork(int workerId, G globalState);
    
    void updateGlobalState(G globalState);
  }
  
  private static final Log LOG = LogFactory.getLog(Worker.class);
  
  private final int workerId;
  private final TaskMasterProxy<T, G> taskMaster;
  private final BranchReduceJob<T, G> job;
  private final BlockingQueue<T> tasks;
  private final Processor<T, G> processor;
  
  private Context context = null;
  
  public Worker(int workerId, TaskMasterProxy<T, G> taskMaster, BranchReduceJob<T, G> job) {
    this.workerId = workerId;
    this.taskMaster = taskMaster;
    this.job = job;
    this.processor = job.constructProcessor();
    this.tasks = new LinkedBlockingQueue<T>();
  }

  public boolean hasStarted() {
    return context != null;
  }
  
  public Class<T> getTaskClass() {
    return job.getTaskClass();
  }

  public Class<G> getGlobalStateClass() {
    return job.getGlobalStateClass();
  }
  
  public void startWork(List<T> newTasks, G globalState) {
    this.context = new Context(job.getConfiguration(), globalState);
    this.tasks.addAll(newTasks);
    startAndWait();
    LOG.info("Started work with " + newTasks.size() + " tasks");
  }
  
  public List<T> giveAwayTasks() {
    List<T> stolen = Lists.newArrayList();
    tasks.drainTo(stolen, (1 + tasks.size()) / 2);
    if (!stolen.isEmpty()) {
      LOG.info("Gave away " + stolen.size() + " tasks");
    }
    return stolen;
  }
  
  public void updateGlobalState(G globalState) {
    if (!context.updateGlobalState(globalState, false)) {
      // We have a better local optima
      taskMaster.updateGlobalState(context.readGlobalState());
    }
  }

  public void sendFinalGlobalStateUpdate() {
    taskMaster.updateGlobalState(context.readGlobalState());
  }
  
  @Override
  protected void startUp() throws Exception {
    processor.initialize(context);
  }
  
  @Override
  protected void run() throws Exception {
    while (true) {
      T task = tasks.poll();
      if (task == null) {
        // Wait for some work to show up.
        List<T> moreWork = taskMaster.getWork(workerId, context.readGlobalState());
        if (moreWork.isEmpty()) {
          // If nothing comes back, we're done.
          stop();
          break;
        }
        tasks.addAll(moreWork.subList(1, moreWork.size()));
        task = moreWork.get(0);
      }
      LOG.info("Now processing task = " + task);
      processor.execute(task, context);
    }
  }
  
  @Override
  protected void shutDown() {
    processor.cleanup(context);
    LOG.info("Finished processing");
  }

  private class Context extends BranchReduceContext<T, G> {

    public Context(Configuration conf, G globalState) {
      super(conf, globalState);
    }
    
    @Override
    public boolean updateGlobalState(G other) {
      return updateGlobalState(other, true);
    }

    public synchronized boolean updateGlobalState(G other, boolean local) {
      G globalState = readGlobalState();
      if (globalState.mergeWith(other)) {
        if (local) {
          taskMaster.updateGlobalState(globalState);
        }
        return true;
      }
      return false;      
    }

    @Override
    public void emit(T... subTasks) {
      tasks.addAll(Arrays.asList(subTasks));
    }
  }
}
