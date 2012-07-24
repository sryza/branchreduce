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
import com.cloudera.branchreduce.impl.distributed.TaskMaster;

public class LordHandler<T extends Writable, G extends GlobalState<G>> implements Lord.Iface {

  private final TaskMaster<T, G> taskMaster;

  private boolean jobFinished = false;
  private boolean finishedNotificationSent = false;
  
  public LordHandler(TaskMaster<T, G> taskMaster) {
    this.taskMaster = taskMaster;
  }
  
  public void signalJobFinished() {
    this.jobFinished = true;
  }
  
  public boolean finishedNotificationSent() {
    return finishedNotificationSent;
  }
  
  @Override
  public VassalRegistrationResponse registerVassal(VassalRegistrationRequest req)
      throws TException {
    VassalProxy<T, G> vp = new VassalProxy<T, G>(req.getHostname(), req.getPort(),
        taskMaster.getTaskClass());
    int id = taskMaster.registerWorker(vp);
    return new VassalRegistrationResponse().setWorkerId(id);
  }

  @Override
  public WorkResponse getWork(WorkRequest req) throws TException {
    List<T> tasks = taskMaster.getWork(req.getWorkerId(),
        Writables.fromByteBuffer(req.bufferForGlobalState(), taskMaster.getGlobalStateClass()));
    WorkResponse resp = new WorkResponse();
    for (T task : tasks) {
      resp.addToTasks(Writables.toByteBuffer(task));
    }
    return resp;
  }

  @Override
  public UpdateGlobalStateResponse updateGlobalState(
      UpdateGlobalStateRequest req) throws TException {
    G globalState = Writables.fromByteBuffer(req.bufferForGlobalState(),
        taskMaster.getGlobalStateClass());
    taskMaster.updateGlobalState(globalState);
    return new UpdateGlobalStateResponse();
  }

  @Override
  public GlobalStatusResponse getGlobalStatus(GlobalStatusRequest req)
      throws TException {
    GlobalStatusResponse resp = new GlobalStatusResponse();
    resp.setGlobalState(Writables.toByteBuffer(taskMaster.getGlobalState()));
    if (jobFinished) {
      resp.setFinished(true);
      finishedNotificationSent = true;
    } else {
      resp.setFinished(false);
    }
    return resp;
  }
}
