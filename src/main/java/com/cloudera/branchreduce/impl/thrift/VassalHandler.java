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
package com.cloudera.branchreduce.impl.thrift;

import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.thrift.TException;

import com.cloudera.branchreduce.GlobalState;
import com.cloudera.branchreduce.impl.distributed.Worker;

public class VassalHandler<T extends Writable, G extends GlobalState<G>> implements Vassal.Iface {

  private final Worker<T, G> worker;
  
  public VassalHandler(Worker<T, G> worker) {
    this.worker = worker;
  }
  
  @Override
  public StartTasksResponse startTasks(StartTasksRequest req) throws TException {
    if (!worker.isRunning()) {
      List<T> tasks = Writables.fromByteBuffer(req.getTasks(), worker.getTaskClass());
      G globalState = Writables.fromByteBuffer(req.bufferForGlobalState(),
          worker.getGlobalStateClass());
      worker.startWork(tasks, globalState);
    }
    return new StartTasksResponse();
  }

  @Override
  public StealWorkResponse stealWork(StealWorkRequest req) throws TException {
    StealWorkResponse resp = new StealWorkResponse();
    if (worker.isRunning()) {
      for (T task : worker.giveAwayTasks()) {
        resp.addToTasks(Writables.toByteBuffer(task));
      }
    }
    return resp;
  }
  
  @Override
  public UpdateGlobalStateResponse updateGlobalState(
      UpdateGlobalStateRequest req) throws TException {
    if (worker.isRunning()) {
      G other = Writables.fromByteBuffer(req.bufferForGlobalState(),
          worker.getGlobalStateClass());
      worker.updateGlobalState(other);
    }
    return new UpdateGlobalStateResponse();
  }
}
