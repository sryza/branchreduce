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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

import com.cloudera.branchreduce.GlobalState;
import com.cloudera.branchreduce.impl.distributed.TaskMaster.WorkerProxy;

/**
 * Responsible for replenishing tasks for nodes that are out of work.
 */
public interface TaskSupplier<T extends Writable, G extends GlobalState<G>> {
  public List<T> getWork(int requestorId, G workerState,
      List<WorkerProxy<T, G>> workers);
  
  public boolean isDone();
  
  public void initialize(Collection<T> initialTasks, int vassalCount,
      TaskMaster<T, G> taskMaster, Configuration conf);
  
  public Map<Integer, List<T>> assignInitialTasks();

}
