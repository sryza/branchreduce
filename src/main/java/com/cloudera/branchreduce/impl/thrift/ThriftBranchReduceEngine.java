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
import org.apache.hadoop.io.Writable;

import com.cloudera.branchreduce.BranchReduceContext;
import com.cloudera.branchreduce.BranchReduceEngine;
import com.cloudera.branchreduce.BranchReduceJob;
import com.cloudera.branchreduce.GlobalState;

/**
 * The Thrift-based implementation of the {@code BranchReduceEngine} strategy.
 */
public class ThriftBranchReduceEngine implements BranchReduceEngine {

  private static final Log LOG = LogFactory.getLog(ThriftBranchReduceEngine.class);
  
  private final String kittenConfigFile;
  private final String branchReduceYarnConfig;
  private final Map<String, Object> env;
  private final Map<String, String> localResources;
  
  public ThriftBranchReduceEngine(String kittenConfigFile, String branchReduceYarnConfig,
      Map<String, Object> env, Map<String, String> localResources) {
    this.kittenConfigFile = kittenConfigFile;
    this.branchReduceYarnConfig = branchReduceYarnConfig;
    this.env = env;
    this.localResources = localResources;
  }
  
  @Override
  public <T extends Writable, G extends GlobalState<G>> BranchReduceContext<T, G> run(
      BranchReduceJob<T, G> job) {
    Client client = new Client(env, localResources);
    client.setConf(job.getConfiguration());
    try {
      client.run(new String[] { kittenConfigFile, branchReduceYarnConfig });
    } catch (Exception e) {
      LOG.error("Error running client branchreduce job", e);
      return null;
    }
    return new BranchReduceContext<T, G>(client.getConf(), (G) client.getGlobalState());
  }

}
