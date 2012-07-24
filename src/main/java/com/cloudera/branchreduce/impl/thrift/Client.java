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

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;

import com.cloudera.branchreduce.BranchReduceConfig;
import com.cloudera.branchreduce.GlobalState;
import com.cloudera.kitten.client.KittenClient;
import com.cloudera.kitten.client.YarnClientService;

/**
 * Kicks off a YARN-based BranchReduce execution with Kitten and monitors it via
 * the Thrift API exposed by the master.
 */
public class Client extends KittenClient {

  private static final Log LOG = LogFactory.getLog(Client.class);
  
  private GlobalState value;
  
  public Client() {
    super();
  }
  
  public Client(Map<String, Object> env) {
    super(env);
  }
  
  public Client(Map<String, Object> env, Map<String, String> resources) {
    super(env, resources);
  }
  
  @Override
  public int handle(YarnClientService clientService) throws Exception {
    clientService.startAndWait();
    if (!clientService.isRunning()) {
      LOG.error("BranchReduce job did not start, exiting...");
      return 1;
    }
    
    Lord.Client client = null;
    while (clientService.isRunning()) {
      ApplicationReport report = clientService.getApplicationReport();
      if (report.getYarnApplicationState() == YarnApplicationState.RUNNING) {
        String originalTrackingUrl = report.getOriginalTrackingUrl();
        if (originalTrackingUrl != null && originalTrackingUrl.contains(":")) {
          System.out.println("Original Tracking URL = " + originalTrackingUrl);
          String[] pieces = originalTrackingUrl.split(":");
          TSocket socket = new TSocket(pieces[0], Integer.valueOf(pieces[1]));
          TProtocol protocol = new TBinaryProtocol(socket);
          client = new Lord.Client(protocol);
          socket.open();
          break;
        }
      }
    }
    
    if (client == null) {
      LOG.error("Could not connect to thrift service to get status");
      return 1;
    }
    
    Configuration conf = clientService.getParameters().getConfiguration();
    Class<GlobalState> globalStatusClass = (Class<GlobalState>) conf.getClass(
        BranchReduceConfig.GLOBAL_STATE_CLASS, GlobalState.class);
    
    boolean finished = false;
    while (!clientService.isApplicationFinished()) {
      if (!finished) {
        GlobalStatusResponse resp = client.getGlobalStatus(new GlobalStatusRequest());
        this.value = Writables.fromByteBuffer(resp.bufferForGlobalState(), globalStatusClass);
        if (resp.isFinished()) {
          LOG.info("Job finished running.");
          finished = true;
        }
        LOG.info(value);
      }
      Thread.sleep(1000);
    }
    
    clientService.stopAndWait();
    ApplicationReport report = clientService.getFinalReport();
    if (report.getFinalApplicationStatus() == FinalApplicationStatus.SUCCEEDED) {
      System.out.println("Job complete.");
      System.out.println(value);
      return 0;
    } else {
      System.out.println("Final app state: " + report.getFinalApplicationStatus());
      System.out.println("Last global state:");
      System.out.println(value);
      return 1;
    }
  }
  
  public GlobalState getGlobalState() {
    return value;
  }
  
  public static void main(String[] args) throws Exception {
    int rc = ToolRunner.run(new Configuration(), new Client(), args);
    System.exit(rc);
  }
}
