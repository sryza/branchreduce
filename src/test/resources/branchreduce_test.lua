
-- YARN/Java configuration via environment variables.
base_env = {
  CLASSPATH = table.concat({"${CLASSPATH}", PWD .. "/target/classes/", PWD .. "/target/lib/*"}, ":"),
}

-- Configuration parameters for BranchReduce based on the kind of BnB job we're executing.
oz_package = "com.cloudera.branchreduce.onezero."
base_conf = {
  ["branchreduce.processor.class"] = oz_package .. "ImplicitEnumerationSolver",
  ["branchreduce.task.class"] = oz_package .. "PartialSolution",
  ["branchreduce.globalstate.class"] = oz_package .. "CurrentBestSolution",
  ["branchreduce.lp.problem"] = LP_PROBLEM -- The actual text of the problem
}

branchreduce = yarn {
  name = "BranchReduce OneZero",
  memory = 256,
  conf = base_conf,

  master = {
    env = base_env,
    command = {
      base = "java -Xmx128m -Dlog4j.configuration=brlog4j/log4j.properties com.cloudera.branchreduce.impl.thrift.LordMain",
      args = { "-conf job.xml" , VASSALS, "1> <LOG_DIR>/stdout 2> <LOG_DIR>/stderr" },
    }
  },

  container = {
    instances = VASSALS,
    env = base_env,
    command = {
      base = "java -Xmx128m -Dlog4j.configuration=brlog4j/log4j.properties com.cloudera.branchreduce.impl.thrift.VassalMain",
      args = { "-conf job.xml", MASTER_HOSTNAME, MASTER_PORT, "1> <LOG_DIR>/stdout 2> <LOG_DIR>/stderr" },
    }
  }
}
