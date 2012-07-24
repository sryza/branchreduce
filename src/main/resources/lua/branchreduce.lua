
-- YARN/Java configuration via environment variables.
H_SHARE = "/usr/lib/hadoop"
COMMON_CP = table.concat({H_SHARE, "/*:", H_SHARE, "/lib/*"}, "")
HDFS_CP = table.concat({H_SHARE, "-hdfs/*:", H_SHARE, "-hdfs/lib/*"}, "")
YARN_CP = table.concat({H_SHARE, "-yarn/*:", H_SHARE, "-yarn/lib/*"}, "")

-- Don't use all of the memory in the container for the task itself.
XMX = MEMORY_PER_TASK / 4

branchreduce = yarn {
  name = JOB_NAME,
  memory = MEMORY_PER_TASK,
  env = {
    CLASSPATH = table.concat({"${CLASSPATH}", COMMON_CP, HDFS_CP, YARN_CP, "./" .. BRANCH_REDUCE_JAR}, ":"),
  },
  resources = {
    -- Where to find the actual JAR file that executes the code.
    { file = BRANCH_REDUCE_JAR_PATH }
  },

  master = {
    command = {
      base = "java -Xmx" .. XMX .. "m -Dlog4j.configuration=brlog4j/log4j.properties com.cloudera.branchreduce.impl.thrift.LordMain",
      args = { "-conf job.xml" , NUM_WORKERS, "1> <LOG_DIR>/stdout 2> <LOG_DIR>/stderr" },
    }
  },

  container = {
    instances = NUM_WORKERS,
    command = {
      base = "java -Xmx" .. XMX .. "m -Dlog4j.configuration=brlog4j/log4j.properties com.cloudera.branchreduce.impl.thrift.VassalMain",
      -- MASTER_HOSTNAME and MASTER_PORT are specified by the appmaster
      args = { "-conf job.xml", MASTER_HOSTNAME, MASTER_PORT, "1> <LOG_DIR>/stdout 2> <LOG_DIR>/stderr" },
    }
  }
}
