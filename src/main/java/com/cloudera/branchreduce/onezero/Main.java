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
package com.cloudera.branchreduce.onezero;

import java.io.File;
import java.io.PrintWriter;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.cloudera.branchreduce.BranchReduceContext;
import com.cloudera.branchreduce.BranchReduceJob;

/**
 * A driver class for the implicit enumeration solver.
 */
public class Main extends Configured implements Tool {

  private static final Options OPTIONS = new Options();
  static {
    OPTIONS
        .addOption("local", false, "Whether to run in local mode")
        .addOption("lpfile", true, "The name of the LP file containing the problem")
        .addOption("workers", true, "The number of worker tasks to create")
        .addOption("memory", true, "The amount of memory to allocate per task")
        .addOption("threads", true, "The number of threads per worker");
    
    OPTIONS.getOption("lpfile").isRequired();
    OPTIONS.getOption("workers").setType(Integer.class);
    OPTIONS.getOption("memory").setType(Integer.class);
    OPTIONS.getOption("threads").setType(Integer.class);
  }
  
  @Override
  public int run(String[] args) throws Exception {
    if (args.length == 0) {
      final PrintWriter writer = new PrintWriter(System.out);  
      final HelpFormatter usageFormatter = new HelpFormatter();  
      usageFormatter.printUsage(writer, 80, "0-1 Implicit Enumeration", OPTIONS);  
      writer.flush();
      return 1;
    }
    
    CommandLine cmdLine = (new GnuParser()).parse(OPTIONS, args);

    boolean runLocally = cmdLine.hasOption("local");
    File lpFile = new File(cmdLine.getOptionValue("lpfile"));
    BranchReduceJob<PartialSolution, CurrentBestSolution> job =
        ImplicitEnumerationSolver.createJob(runLocally, lpFile, getConf());
    job.setJobName("BranchReduce: Implicit Enumeration of: " + lpFile.getName());
    
    if (cmdLine.hasOption("workers")) {
      job.setNumWorkers(new Integer(cmdLine.getOptionValue("workers")));
    }
    if (cmdLine.hasOption("threads")) {
      job.setNumThreads(new Integer(cmdLine.getOptionValue("threads")));
    }
    if (cmdLine.hasOption("memory")) {
      job.setMemory(new Integer(cmdLine.getOptionValue("memory")));
    }
    
    BranchReduceContext<PartialSolution, CurrentBestSolution> ctxt = job.submit();
    if (ctxt == null) {
      return 1;
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int rc = ToolRunner.run(new Configuration(), new Main(), args);
    System.exit(rc);
  }
}
