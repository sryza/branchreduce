namespace java com.cloudera.branchreduce.impl.thrift

struct VassalRegistrationRequest {
  1: string hostname,
  2: i32 port
}

struct VassalRegistrationResponse {
  1: i32 workerId,
}

struct WorkRequest {
  1: i32 workerId,
  2: binary globalState
}

struct WorkResponse {
  1: list<binary> tasks,
}

struct UpdateGlobalStateRequest {
  1: binary globalState
}

struct UpdateGlobalStateResponse {
}

struct GlobalStatusRequest {
  // Empty for now.
}

struct GlobalStatusResponse {
  1: binary globalState,
  2: bool finished
}

service Lord {
  VassalRegistrationResponse registerVassal(1: VassalRegistrationRequest req),
  WorkResponse getWork(1: WorkRequest req),
  UpdateGlobalStateResponse updateGlobalState(1: UpdateGlobalStateRequest req),
  GlobalStatusResponse getGlobalStatus(1: GlobalStatusRequest req)
}

struct StartTasksRequest {
  1: list<binary> tasks,
  2: binary globalState
}

struct StartTasksResponse {
  // Empty for now.
}

struct StealWorkRequest {
  // Empty for now.
}

struct StealWorkResponse {
  1: list<binary> tasks
}

service Vassal {
  UpdateGlobalStateResponse updateGlobalState(1: UpdateGlobalStateRequest req),
  StartTasksResponse startTasks(1: StartTasksRequest req),
  StealWorkResponse stealWork(1: StealWorkRequest req)
}
