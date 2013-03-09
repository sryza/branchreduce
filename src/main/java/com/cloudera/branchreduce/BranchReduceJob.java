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
package com.cloudera.branchreduce;

import java.io.File;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;

import com.cloudera.branchreduce.impl.local.MultiThreadedBranchReduceEngine;
import com.cloudera.branchreduce.impl.local.SingleThreadedBranchReduceEngine;
import com.cloudera.branchreduce.impl.thrift.ThriftBranchReduceEngine;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 *
 */
public class BranchReduceJob<T extends Writable, G extends GlobalState<G>> implements BranchReduceConfig {

  private static final Log LOG = LogFactory.getLog(BranchReduceJob.class);
  
  private static final String DEFAULT_KITTEN_CONFIG_FILE = "lua/branchreduce.lua";
  
  private final Configuration conf;
  private final boolean runLocally;

  private final Map<String, Object> env;
  private final Map<String, String> resources;
  private String kittenConfigFile;
  private String kittenJob = "branchreduce";

  public BranchReduceJob(boolean runLocally) {
    this(runLocally, new Configuration(), DEFAULT_KITTEN_CONFIG_FILE);
  }
  
  public BranchReduceJob(boolean runLocally, Configuration conf) {
    this(runLocally, conf, DEFAULT_KITTEN_CONFIG_FILE);
  }
  
  public BranchReduceJob(boolean runLocally, Configuration conf,
      String kittenConfigFile) {
    this.conf = conf;
    this.env = Maps.newHashMap();
    this.resources = Maps.newHashMap();
    this.runLocally = runLocally;
    this.kittenConfigFile = kittenConfigFile;
    initEnv();
  }
  
  private void initEnv() {
    env.put(NUM_THREADS, 1);
    env.put(NUM_WORKERS, 2);
    env.put(MEMORY_PER_TASK, 512);
    env.put(JOB_NAME, "BranchReduce Job");    
  }
  
  public Configuration getConfiguration() {
    return conf;
  }
  
  public BranchReduceJob<T, G> addFileResource(File file) {
    return addResource(file.getName(), file.getAbsolutePath());
  }
  
  public BranchReduceJob<T, G> addResource(String name, String filePath) {
    this.resources.put(name, filePath);
    return this;
  }
  
  public BranchReduceJob<T, G> setHadoopHome(String hadoopHome) {
    env.put(HADOOP_HOME, hadoopHome);
    return this;
  }
  
  public BranchReduceJob<T, G> setJarByClass(Class<?> clazz) {
    String jarPath = JobConf.findContainingJar(clazz);
    if (jarPath != null) {
      File jarFile = new File(jarPath);
      env.put(BRANCH_REDUCE_JAR_PATH, jarPath);
      env.put(BRANCH_REDUCE_JAR, jarFile.getName());
    } else {
      LOG.warn("branch reduce jar file not set; class files may not be found");
    }
    return this;
  }
  
  public BranchReduceJob<T, G> setProcessorClass(Class<? extends Processor<T, G>> processorClass) {
    conf.setClass(PROCESSOR_CLASS, processorClass, Processor.class);
    return this;
  }
  
  public BranchReduceJob<T, G> setTaskClass(Class<T> taskClass) {
    conf.setClass(TASK_CLASS, taskClass, Writable.class);
    return this;
  }
  
  public BranchReduceJob<T, G> setGlobalStateClass(Class<G> globalStateClass) {
    conf.setClass(GLOBAL_STATE_CLASS, globalStateClass, GlobalState.class);
    return this;
  }
  
  public BranchReduceJob<T, G> setNumWorkers(int numWorkers) {
    Preconditions.checkArgument(numWorkers > 0);
    env.put(NUM_WORKERS, numWorkers);
    return this;
  }
  
  public BranchReduceJob<T, G> setNumThreads(int numThreads) {
    Preconditions.checkArgument(numThreads > 0);
    env.put(NUM_THREADS, numThreads);
    return this;
  }
  
  public BranchReduceJob<T, G> setMemory(int megabytes) {
    Preconditions.checkArgument(megabytes > 0);
    env.put(MEMORY_PER_TASK, megabytes);
    return this;
  }
  
  public BranchReduceJob<T, G> setJobName(String jobName) {
    env.put(JOB_NAME, jobName);
    return this;
  }
  
  public boolean isLocal() {
    return runLocally;
  }
  
  public BranchReduceContext<T, G> submit() {
    BranchReduceEngine engine = null;
    if (runLocally) {
      int threads = (Integer) env.get(NUM_THREADS);
      engine = threads == 1 ? new SingleThreadedBranchReduceEngine() :
        new MultiThreadedBranchReduceEngine(threads);
    } else {
      engine = new ThriftBranchReduceEngine(kittenConfigFile, kittenJob,
          env, resources);
    }
    
    return engine.run(this);
  }
  
  @SuppressWarnings("unchecked")
  public Class<G> getGlobalStateClass() {
    return (Class<G>) conf.getClass(GLOBAL_STATE_CLASS, null);
  }
  
  @SuppressWarnings("unchecked")
  public Class<T> getTaskClass() {
    return (Class<T>) conf.getClass(TASK_CLASS, null);
  }
  
  @SuppressWarnings("unchecked")
  public Processor<T, G> constructProcessor() {
    return (Processor<T, G>) ReflectionUtils.newInstance(
        conf.getClass(PROCESSOR_CLASS, null), conf);
  }
  
  @SuppressWarnings("unchecked")
  public T constructInitialTask() {
    return (T) ReflectionUtils.newInstance(
        conf.getClass(TASK_CLASS, null), conf);
  }
  
  @SuppressWarnings("unchecked")
  public G constructGlobalState() {
    return (G) ReflectionUtils.newInstance(
        conf.getClass(GLOBAL_STATE_CLASS, null), conf);
  }
}
