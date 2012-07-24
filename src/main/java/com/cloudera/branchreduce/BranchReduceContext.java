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

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

/**
 * The context used by a {@code Processor} instance to read and write the current state of the system and
 * emit new tasks to be processed.
 */
public class BranchReduceContext<T extends Writable, G extends GlobalState<G>> {
  private final Configuration conf;
  private final G globalState;
  private final BlockingQueue<T> tasks;
  
  public BranchReduceContext(Configuration conf, G globalState) {
    this(conf, globalState, new LinkedBlockingQueue<T>());
  }
  
  public BranchReduceContext(Configuration conf, G globalState, BlockingQueue<T> tasks) {
    this.conf = conf;
    this.globalState = globalState;
    this.tasks = tasks;
  }
  
  public Configuration getConfiguration() {
    return conf;
  }
  
  public final G readGlobalState() {
    return globalState;
  }
  
  public boolean updateGlobalState(G other) {
    return readGlobalState().mergeWith(other);
  }
  
  public void emit(T... subTasks) {
    emit(Arrays.asList(subTasks));
  }

  public void emit(Collection<T> subTasks) {
    tasks.addAll(subTasks);
  }
  
  public BlockingQueue<T> getTaskQueue() {
    return tasks;
  }
  
  public boolean isTaskQueueEmpty() {
    return tasks.isEmpty();
  }
  
  public T take() {
    try {
      return tasks.take();
    } catch (InterruptedException e) {
      return null;
    }
  }
}
