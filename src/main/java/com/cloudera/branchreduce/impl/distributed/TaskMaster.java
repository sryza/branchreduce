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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;

import com.cloudera.branchreduce.GlobalState;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractScheduledService;

/**
 *
 */
public class TaskMaster<T extends Writable, G extends GlobalState<G>> extends AbstractScheduledService {

  public interface WorkerProxy<T, G> {
    void startTasks(List<T> tasks, G globalState);
    
    List<T> getTasks();
    
    void updateGlobalState(G globalState);
  }

  private static final Log LOG = LogFactory.getLog(TaskMaster.class);
  
  private final int vassalCount;
  private final Class<T> taskClass;
  private final ExecutorService executor;

  private final List<WorkerProxy<T, G>> workers;
  
  private final G globalState;
  private boolean sendGlobalStateUpdate = false;
  private boolean hasStarted = false;
  
  private TaskSupplier<T, G> taskSupplier;
  
  public TaskMaster(int vassalCount, List<T> initialTasks, G globalState,
      TaskSupplier<T, G> taskSupplier) {
    this(vassalCount, initialTasks, globalState, Executors.newCachedThreadPool(),
        taskSupplier);
  }
  
  public TaskMaster(int vassalCount, List<T> initialTasks, G globalState,
      ExecutorService executor, TaskSupplier<T, G> taskSupplier) {
    this.vassalCount = vassalCount;
    this.workers = Lists.newArrayList();
    this.globalState = globalState;
    this.taskSupplier = taskSupplier;
    if (!initialTasks.isEmpty()) {
      this.taskClass = (Class<T>) initialTasks.get(0).getClass();
    } else {
      this.taskClass = null;
    }
    this.executor = executor;
  }
  
  public Class<T> getTaskClass() {
    return taskClass;
  }
  
  public Class<G> getGlobalStateClass() {
    return (Class<G>) globalState.getClass();
  }
  
  public G getGlobalState() {
    return globalState;
  }
  
  public boolean hasStarted() {
    return hasStarted;
  }
  
  public List<WorkerProxy<T, G>> getWorkers() {
    return workers;
  }
  
  public int registerWorker(final WorkerProxy<T, G> worker) {
    int id = -1;
    
    synchronized(workers) {
      id = workers.size();
      workers.add(worker);
      if (workers.size() == vassalCount) {
        start();
      }
    }
    
    LOG.info("Registered worker no. " + (id + 1));
    return id;
  }
  
  public List<T> getWork(int requestorId, G workerState) {
    updateGlobalState(workerState);
    return taskSupplier.getWork(requestorId, workerState, workers);
  }

  public boolean updateGlobalState(G other) {
    LOG.info("Received global state update: " + other);
    boolean ret = false;
    synchronized(globalState) {
      if (globalState.mergeWith(other)) {
        this.sendGlobalStateUpdate = true;
        ret = true;
        LOG.info("New global state: " + globalState);
      }
    }
    return ret;
  }
  
  /**
   * To be called by the task supplier
   */
  public void abortTask(int vassalId, T task) {
    // TODO
  }
  
  @Override
  protected void runOneIteration() throws Exception {
    this.hasStarted = true;
    synchronized(globalState) {
      if (sendGlobalStateUpdate) {
        // Send notifications to all of the workers.
        for (final WorkerProxy<T, G> worker : workers) {
          executor.submit(new Runnable() {
            @Override
            public void run() {
              worker.updateGlobalState(globalState);
            }
          });
        }
        this.sendGlobalStateUpdate = false;
      }
    }
    
    if (taskSupplier.isDone()) {
      LOG.info("Nothing to do, stopping");
      stop();
    }
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(1, 1, TimeUnit.SECONDS);
  }

  @Override
  protected void startUp() throws Exception {
    Map<Integer, List<T>> initialTasks = taskSupplier.assignInitialTasks();
    if (initialTasks.isEmpty()) {
      LOG.info("No tasks to perform, exiting");
      stop();
      return;
    } else {
      LOG.info("Initial task count = " + initialTasks.size());
    }
    
    // Send tasks to all of the workers.
    for (final Map.Entry<Integer, List<T>> work : initialTasks.entrySet()) {
      final WorkerProxy<T, G> worker = workers.get(work.getKey());
      LOG.info("Starting " + work.getValue().size() + " units at worker "
          + work.getKey());
      executor.submit(new Runnable() {
        @Override
        public void run() {
          worker.startTasks(work.getValue(), globalState);
        }
      });
    }
  }

  @Override
  protected void shutDown() throws Exception {
    // No cleanup, surprisingly.
  }
}
