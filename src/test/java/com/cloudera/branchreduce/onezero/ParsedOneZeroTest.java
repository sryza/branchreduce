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

import static org.junit.Assert.assertEquals;

import java.io.File;

import org.junit.Before;
import org.junit.Test;

import com.cloudera.branchreduce.BranchReduceEngine;
import com.cloudera.branchreduce.BranchReduceJob;
import com.cloudera.branchreduce.impl.local.MultiThreadedBranchReduceEngine;
import com.cloudera.branchreduce.impl.local.SingleThreadedBranchReduceEngine;
import com.google.common.io.Files;
import com.google.common.io.Resources;

/**
 * Tests out the simplified (?!) LP file parser.
 */
public class ParsedOneZeroTest {
  
  private File tmpLpFile;
  private BranchReduceJob<PartialSolution, CurrentBestSolution> job;
  
  @Before
  public void testParseFile() throws Exception {
    this.tmpLpFile = File.createTempFile("simple", ".lp");
    Files.copy(Resources.newInputStreamSupplier(Resources.getResource("test.lp")), tmpLpFile);
    tmpLpFile.deleteOnExit();
    
    this.job = ImplicitEnumerationSolver.createJob(true, tmpLpFile);
    
  }
  
  @Test
  public void testSingleThreaded() throws Exception {
    exec(new SingleThreadedBranchReduceEngine(), 17);
  }
  
  @Test
  public void testMultiThreaded() throws Exception {
    exec(new MultiThreadedBranchReduceEngine(4), 17);
  }
  
  public void exec(BranchReduceEngine engine, int expected) {
    assertEquals(expected, engine.run(job).readGlobalState().getValue());
  }
}
