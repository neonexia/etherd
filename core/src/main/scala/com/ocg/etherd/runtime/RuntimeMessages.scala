package com.ocg.etherd.runtime

import com.ocg.etherd.topology.Stage

object RuntimeMessages {

  // Client --> ClusterManager
  case class SubmitStages(topologyName: String, stages: List[Stage]) extends Serializable

  //ClusterManager --> TopologyExecutionManager
  case class ScheduleStages(stages: List[Stage])

  // TopologyExecutionManager --> Executor
  case class Report(topologyName: String)
  case class ExecuteStage(stage: Stage)

  // Executor --> TopologyExecutionManager
  case class ExecutorData(executorId: String, stageId:Int, host: String, port: Int) extends Serializable

  case class RegisterExecutor(topologyName: String, executorData: ExecutorData) extends Serializable

  case class ExecutorHeartBeat(executorId: String) extends Serializable

  case class StageExecutionScheduled(executorId: String, topologyName: String, stageId: Int)

  //Debugging
  case class GetRegisteredExecutors(topologyName: String) extends Serializable
  case class ExecutorList(executors: List[ExecutorData]) extends Serializable
}
