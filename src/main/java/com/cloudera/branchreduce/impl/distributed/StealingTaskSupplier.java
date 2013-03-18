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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;

import com.cloudera.branchreduce.GlobalState;
import com.cloudera.branchreduce.impl.distributed.TaskMaster.WorkerProxy;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class StealingTaskSupplier<T extends Writable, G extends GlobalState<G>>
    implements TaskSupplier<T, G> {

  private static final Log LOG = LogFactory.getLog(TaskSupplier.class);

  private final BlockingQueue<T> tasks;
  private final Map<Integer, Boolean> hasWork;
  private int vassalCount;
  
  public StealingTaskSupplier() {
    this.hasWork = Maps.newConcurrentMap();
    this.tasks = new LinkedBlockingQueue<T>();
  }
  
  @Override
  public void initialize(Collection<T> initialTasks, int vassalCount,
      TaskMaster<T, G> master) {
    tasks.addAll(initialTasks);
    this.vassalCount = vassalCount;
  }
  
  @Override
  public List<T> getWork(int requestorId, G workerState,
      List<WorkerProxy<T, G>> workers) {
    if (!tasks.isEmpty()) {
      try {
        List<T> ret = ImmutableList.of(tasks.take());
        LOG.info("Sending work to " + requestorId);
        return ret;
      } catch (InterruptedException e) {
        return ImmutableList.of();
      }
    } else {
      // Need to get some more work, if anyone has any.
      Set<Integer> workingIds = Sets.newHashSet(hasWork.keySet());
      for (Integer workingId : workingIds) {
        if (workingId != requestorId) {
          WorkerProxy<T, G> worker = workers.get(workingId);
          List<T> stolen = worker.getTasks();
          if (!stolen.isEmpty()) {
            List<T> ret = ImmutableList.of(stolen.get(0));
            tasks.addAll(stolen.subList(1, stolen.size()));
            LOG.info("Sending stolen work to " + requestorId);
            return ret;
         }
        }
      }
      LOG.info("Could not send work to " + requestorId);
      hasWork.remove(requestorId);
      return ImmutableList.of();
    }
  }
  
  public boolean isDone() {
    return tasks.isEmpty() && hasWork.isEmpty();
  }
  
  public Map<Integer, List<T>> assignInitialTasks() {
    Map<Integer, List<T>> initialAssignment = new HashMap<Integer, List<T>>();
    for (int i = 0; i < vassalCount && !tasks.isEmpty(); i++) {
      initialAssignment.put(i, ImmutableList.of(tasks.remove()));
      hasWork.put(i, true);
    }
    return initialAssignment;
  }
}
