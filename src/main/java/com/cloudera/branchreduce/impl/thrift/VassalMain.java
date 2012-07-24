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
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;

import com.cloudera.branchreduce.BranchReduceJob;
import com.cloudera.branchreduce.impl.distributed.Worker;

/**
 *
 */
public class VassalMain extends Configured implements Tool {

  private static final Log LOG = LogFactory.getLog(VassalMain.class);
  
  private String hostname;
  private ServerSocket socket;

  private BranchReduceJob job;
  
  private String masterHostname;
  private int masterPort;
  
  private void initialize(String[] args) throws Exception {
    this.hostname = InetAddress.getLocalHost().getHostName();
    this.socket = new ServerSocket(0);
    this.job = new BranchReduceJob(false, getConf());
    this.masterHostname = args[0];
    this.masterPort = Integer.valueOf(args[1]);
  }
  
  @Override
  public int run(String[] args) throws Exception {
    LOG.info("Vassal args: " + Arrays.asList(args));
    initialize(args);
    
    TServerSocket serverTransport = new TServerSocket(socket);
    LordProxy lord = new LordProxy(masterHostname, masterPort, job.getTaskClass());
    int workerId = lord.registerWorker(hostname, socket.getLocalPort());
    Worker worker = new Worker(workerId, lord, job);
    VassalHandler handler = new VassalHandler(worker);
    Vassal.Processor vassalProc = new Vassal.Processor(handler);

    final TServer thriftServer = new TThreadPoolServer(
        new TThreadPoolServer.Args(serverTransport).processor(vassalProc));
    Thread thriftServerThread = new Thread("Vassal Thrift Server") {
      @Override
      public void run() { thriftServer.serve(); };
    };
    thriftServerThread.start();
    
    while (!worker.hasStarted() || worker.isRunning()) {
      Thread.sleep(1000);
    }
    worker.sendFinalGlobalStateUpdate();
    LOG.info("Worker finished");    
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    int rc = ToolRunner.run(new Configuration(), new VassalMain(), args);
    System.exit(rc);
  }
}
