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
import com.cloudera.branchreduce.impl.distributed.Worker.TaskMasterProxy;
import com.google.common.collect.ImmutableList;

public class LordProxy<T extends Writable, G extends GlobalState<G>> implements TaskMasterProxy<T, G> {

  private static final Log LOG = LogFactory.getLog(LordProxy.class);
  
  private final Lord.Client client;
  private final Class<T> taskClass;
  
  public LordProxy(String hostname, int port, Class<T> taskClass) throws TTransportException {
    TSocket socket = new TSocket(hostname, port);
    TProtocol protocol = new TBinaryProtocol(socket);
    this.client = new Lord.Client(protocol);
    socket.open();
    this.taskClass = taskClass;
  }
  
  @Override
  public int registerWorker(String hostname, int port) {
    VassalRegistrationRequest req = new VassalRegistrationRequest();
    req.setHostname(hostname).setPort(port);
    
    try {
      return client.registerVassal(req).getWorkerId();
    } catch (TException e) {
      LOG.error("Transport exception registering vassal", e);
    }
    return -1;
  }

  @Override
  public List<T> getWork(int workerId, G globalState) {
    WorkRequest req = new WorkRequest();
    req.setWorkerId(workerId);
    req.setGlobalState(Writables.toByteBuffer(globalState));
    WorkResponse resp = null;
    try {
      resp = client.getWork(req);
    } catch (TException e) {
      LOG.error("Transport exception requesting work", e);
      return ImmutableList.of();
    }
    
    if (resp == null || resp.getTasks() == null || resp.getTasks().isEmpty()) {
      return ImmutableList.of();
    }
    return Writables.fromByteBuffer(resp.getTasks(), taskClass);
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
}
