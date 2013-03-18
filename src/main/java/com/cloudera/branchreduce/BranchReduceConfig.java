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

import com.cloudera.branchreduce.impl.distributed.StealingTaskSupplier;

/**
 * A namespace for BranchReduce configuration parameters.
 */
public interface BranchReduceConfig {
  public static final String PROCESSOR_CLASS = "branchreduce.processor.class";
  public static final String TASK_CLASS = "branchreduce.task.class";
  public static final String GLOBAL_STATE_CLASS = "branchreduce.globalstate.class";
  public static final String TASK_SUPPLIER_CLASS = "branchreduce.tasksupplier.class";
  @SuppressWarnings("rawtypes")
  public static final Class DEFAULT_TASK_SUPPLIER_CLASS = StealingTaskSupplier.class;
  
  public static final String NUM_WORKERS = "NUM_WORKERS";
  public static final String NUM_THREADS = "NUM_THREADS";
  public static final String MEMORY_PER_TASK = "MEMORY_PER_TASK";
  public static final String BRANCH_REDUCE_JAR = "BRANCH_REDUCE_JAR";
  public static final String BRANCH_REDUCE_JAR_PATH = "BRANCH_REDUCE_JAR_PATH";
  public static final String JOB_NAME = "JOB_NAME";
  public static final String HADOOP_HOME = "HADOOP_HOME";
}
