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

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

import com.cloudera.branchreduce.GlobalState;
import com.cloudera.branchreduce.impl.distributed.TaskMaster.WorkerProxy;
import com.google.common.collect.ImmutableList;

public class VassalProxy<T extends Writable, G extends GlobalState<G>> implements WorkerProxy<T, G> {

  private static final Log LOG = LogFactory.getLog(VassalProxy.class);
  
  private final Vassal.Client client;
  private final Class<T> taskClass;
  
  public VassalProxy(String hostname, int port, Class<T> taskClass) throws TTransportException {
    TSocket socket = new TSocket(hostname, port);
    TProtocol protocol = new TBinaryProtocol(socket);
    this.client = new Vassal.Client(protocol);
    socket.open();
    this.taskClass = taskClass;
  }
  
  @Override
  public synchronized void startTasks(List<T> tasks, G globalState) {
    StartTasksRequest req = new StartTasksRequest();
    req.setGlobalState(WritableUtils.toByteArray(globalState));
    for (T task : tasks) {
      req.addToTasks(ByteBuffer.wrap(WritableUtils.toByteArray(task)));
    }
    try {
      client.startTasks(req);
    } catch (TException e) {
      LOG.error("Transport exception starting tasks", e);
    }
  }

  @Override
  public synchronized void updateGlobalState(G globalState) {
    UpdateGlobalStateRequest req = new UpdateGlobalStateRequest();
    req.setGlobalState(WritableUtils.toByteArray(globalState));
    try {
      client.updateGlobalState(req);
    } catch (TException e) {
      LOG.error("Transport exception updating global state", e);
    }
  }

  @Override
  public synchronized List<T> getTasks() {
    try {
      StealWorkResponse resp = client.stealWork(new StealWorkRequest());
      return Writables.fromByteBuffer(resp.getTasks(), taskClass);
    } catch (TException e) {
      LOG.error("Transport exception stealing work", e);
    }
    return ImmutableList.of();
  }
  
}
