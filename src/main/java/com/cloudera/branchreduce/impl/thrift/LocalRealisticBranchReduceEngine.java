package com.cloudera.branchreduce.impl.thrift;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.thrift.TException;

import com.cloudera.branchreduce.BranchReduceContext;
import com.cloudera.branchreduce.BranchReduceEngine;
import com.cloudera.branchreduce.BranchReduceJob;
import com.cloudera.branchreduce.GlobalState;
import com.cloudera.branchreduce.impl.distributed.TaskMaster;
import com.cloudera.branchreduce.impl.distributed.TaskSupplier;
import com.cloudera.branchreduce.impl.distributed.Worker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Service.State;

public class LocalRealisticBranchReduceEngine implements BranchReduceEngine {
  
  private static final Log LOG = LogFactory.getLog(LocalRealisticBranchReduceEngine.class);

  private static final int CHECK_FOR_COMPLETION_INTERVAL = 200;
  
  private TaskMaster taskMaster;
  private int numVassals;
  private Class taskClass;
  private Configuration conf;
  
  public LocalRealisticBranchReduceEngine(int numVassals, Configuration conf) {
    this.numVassals = numVassals;
    this.conf = conf;
  }
  
  @Override
  public <T extends Writable, G extends GlobalState<G>> BranchReduceContext<T, G> run(
      BranchReduceJob<T, G> job) {
    taskClass = job.getTaskClass();
    TaskSupplier<T, G> taskSupplier = job.constructTaskSupplier();
    List<T >initialTasks = ImmutableList.of(job.constructInitialTask());
    TaskMaster<T, G> taskMaster = new TaskMaster<T, G>(numVassals, initialTasks,
        job.constructGlobalState(), taskSupplier);
    taskSupplier.initialize(initialTasks, numVassals, taskMaster, conf);
    LocalLordHandler<T, G> lordHandler = new LocalLordHandler<T, G>(taskMaster);
    
    LocalLordProxy<T, G> lordProxy = new LocalLordProxy<T, G>(lordHandler);
    Map<Integer, VassalHandler<T, G>> vassalHandlers = Maps.newHashMap();
    for (int i = 0; i < numVassals; i++) {
      int vassalId = lordProxy.registerWorker("", 0);
      Worker<T, G> worker = new Worker<T, G>(vassalId, lordProxy, job);
      VassalHandler<T, G> vassalHandler = new VassalHandler<T, G>(worker);
      vassalHandlers.put(vassalId, vassalHandler);
    }
    
    Map<Integer, LocalVassalProxy<T, G>> vassalProxysById = lordHandler.getVassalsById();
    for (int vassalId : vassalHandlers.keySet()) {
      vassalProxysById.get(vassalId).setVassalHandler(vassalHandlers.get(vassalId));
    }

    while (taskMaster.state() != State.TERMINATED) {
      try {
        Thread.sleep(CHECK_FOR_COMPLETION_INTERVAL);
      } catch (InterruptedException ex) {
        LOG.warn("Interrupted while waiting to check for completion");
      }
    }
    
    return new BranchReduceContext<T, G>(job.getConfiguration(), taskMaster.getGlobalState());
  }
  
  
  private class LocalVassalProxy<T extends Writable, G extends GlobalState<G>>
      implements TaskMaster.WorkerProxy<T, G> {

    private VassalHandler<T, G> handler;
    
    public void setVassalHandler(VassalHandler<T, G> handler) {
      this.handler = handler;
    }
    
    @Override
    public void startTasks(List<T> tasks, G globalState) {
      StartTasksRequest req = new StartTasksRequest();
      req.setGlobalState(WritableUtils.toByteArray(globalState));
      for (T task : tasks) {
        req.addToTasks(ByteBuffer.wrap(WritableUtils.toByteArray(task)));
      }
      try {
        handler.startTasks(req);
      } catch (TException ex) {
        LOG.error("Wack that we have an exception here", ex);
      }
    }

    @Override
    public List<T> getTasks() {
      try {
        StealWorkResponse resp = handler.stealWork(new StealWorkRequest());
        return Writables.fromByteBuffer(resp.getTasks(), taskClass);
      } catch (TException e) {
        LOG.error("Transport exception stealing work", e);
      }
      return ImmutableList.of();
    }

    @Override
    public void updateGlobalState(G globalState) {
      UpdateGlobalStateRequest req = new UpdateGlobalStateRequest();
      req.setGlobalState(WritableUtils.toByteArray(globalState));
      try {
        handler.updateGlobalState(req);
      } catch (TException e) {
        LOG.error("Transport exception updating global state", e);
      }      
    }
  }

  private class LocalLordProxy<T extends Writable, G extends GlobalState<G>>
      implements Worker.TaskMasterProxy<T, G> {
    private LordHandler<T, G> handler;
    
    public LocalLordProxy(LordHandler<T, G> handler) {
      this.handler = handler;
    }
    
    @Override
    public int registerWorker(String hostname, int port) {
      VassalRegistrationRequest req = new VassalRegistrationRequest();
      req.setHostname(hostname).setPort(port);
      
      try {
        return handler.registerVassal(req).getWorkerId();
      } catch (TException ex) {
        LOG.error("Wack that we have an exception here", ex);
      }
      return -1;
    }

    @Override
    public List getWork(int workerId, G globalState) {
      WorkRequest req = new WorkRequest();
      req.setWorkerId(workerId);
      req.setGlobalState(Writables.toByteBuffer(globalState));
      WorkResponse resp = null;
      try {
        resp = handler.getWork(req);
      } catch (TException ex) {
        LOG.error("Wack that we have an exception here", ex);
      }
      
      if (resp == null || resp.getTasks() == null || resp.getTasks().isEmpty()) {
        return ImmutableList.of();
      }
      return Writables.fromByteBuffer(resp.getTasks(), taskClass);
    }

    @Override
    public void updateGlobalState(G globalState) {
      UpdateGlobalStateRequest req = new UpdateGlobalStateRequest();
      req.setGlobalState(WritableUtils.toByteArray(globalState));
      try {
        handler.updateGlobalState(req);
      } catch (TException ex) {
        LOG.error("Wack that we have an exception here", ex);
      }
    }
  }
  
  private class LocalLordHandler<T extends Writable, G extends GlobalState<G>>
      extends LordHandler<T, G> {
    private Map<Integer, LocalVassalProxy<T, G>> vassalsById = Maps.newHashMap();
    
    public LocalLordHandler(TaskMaster<T, G> taskMaster) {
      super(taskMaster);
    }
    
    @Override
    public VassalRegistrationResponse registerVassal(VassalRegistrationRequest req) {
      LocalVassalProxy<T, G> vp = new LocalVassalProxy<T, G>();
      int workerId = taskMaster.registerWorker(vp);
      vassalsById.put(workerId, vp);
      return new VassalRegistrationResponse().setWorkerId(workerId);
    }
    
    public Map<Integer, LocalVassalProxy<T, G>> getVassalsById() {
      return vassalsById;
    }
  }
}
