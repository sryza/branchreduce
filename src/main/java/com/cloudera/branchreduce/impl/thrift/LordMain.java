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

import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;

import com.cloudera.branchreduce.BranchReduceContext;
import com.cloudera.branchreduce.BranchReduceJob;
import com.cloudera.branchreduce.GlobalState;
import com.cloudera.branchreduce.Processor;
import com.cloudera.branchreduce.impl.distributed.TaskMaster;
import com.cloudera.kitten.appmaster.ApplicationMasterParameters;
import com.cloudera.kitten.appmaster.ApplicationMasterService;
import com.cloudera.kitten.appmaster.params.lua.LuaApplicationMasterParameters;
import com.cloudera.kitten.appmaster.service.ApplicationMasterServiceImpl;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class LordMain extends Configured implements Tool {
  
  private static final Log LOG = LogFactory.getLog(LordMain.class);
  
  private String hostname;
  private ServerSocket socket;
  private int numVassals;
  
  private BranchReduceJob job;
  private BranchReduceContext context;
  private GlobalState globalState;
  private List initialTasks;
  
  private void initialize(String[] args) throws Exception {
    this.hostname = InetAddress.getLocalHost().getHostName();
    this.socket = new ServerSocket(0);
    this.numVassals = Integer.valueOf(args[0]);
    this.job = new BranchReduceJob(false, getConf());
    createInitialTasks();
  }
  
  @SuppressWarnings({"unchecked", "rawtypes"})
  private void createInitialTasks() {
    this.globalState = job.constructGlobalState();
    this.context = new BranchReduceContext(job.getConfiguration(), globalState);
    Processor proc = job.constructProcessor();
    proc.initialize(context);
    Writable task = null;
    do {
      if (task == null) {
        task = job.constructInitialTask();
      } else {
        task = context.take();
      }
      proc.execute(task, context);
      if (context.isTaskQueueEmpty()) {
        break;
      }
    } while (context.getTaskQueue().size() < numVassals);
    
    if (!context.isTaskQueueEmpty()) {
      this.initialTasks = Lists.newArrayList(context.getTaskQueue());
    } else {
      this.initialTasks = ImmutableList.of();
    }
    
    proc.cleanup(context);
  }
  
  @Override
  public int run(String[] args) throws Exception {
    LOG.info("Initializing lord...");
    initialize(args);
    LOG.info("Lord initialized.");
    
    ApplicationMasterParameters appMasterParams = new LuaApplicationMasterParameters(getConf(),
        ImmutableMap.<String, Object>of("MASTER_HOSTNAME", hostname, "MASTER_PORT", socket.getLocalPort()))
    .setClientPort(socket.getLocalPort())
    .setHostname(hostname);
    ApplicationMasterService appMasterService = new ApplicationMasterServiceImpl(appMasterParams);
    LOG.info("Starting application master service");
    appMasterService.startAndWait();
    
    TaskMaster taskMaster = new TaskMaster(numVassals, initialTasks, globalState);
    LordHandler lordHandler = new LordHandler(taskMaster);
    TServerSocket serverTransport = new TServerSocket(socket);
    Lord.Processor lordProc = new Lord.Processor(lordHandler);
    final TServer thriftServer = new TThreadPoolServer(
        new TThreadPoolServer.Args(serverTransport).processor(lordProc));

    LOG.info("Starting lord thrift server");
    Thread thriftServerThread = new Thread("Lord Thrift Server") {
      @Override
      public void run() { thriftServer.serve(); };
    };
    thriftServerThread.start();
    
    do {
      Thread.sleep(1000);
    } while (appMasterService.hasRunningContainers());

    // Send final notifications
    lordHandler.signalJobFinished();
    while (!lordHandler.finishedNotificationSent()) {
      Thread.sleep(1000);
    }
    thriftServerThread.join(1000);

    LOG.info("Stopping application master service");
    appMasterService.stopAndWait();
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int rc = ToolRunner.run(new Configuration(), new LordMain(), args);
    System.exit(rc);
  }
}
