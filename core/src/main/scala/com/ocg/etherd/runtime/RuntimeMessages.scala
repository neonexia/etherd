package com.ocg.etherd.runtime

import com.ocg.etherd.runtime.scheduler.SchedulableTask
import com.ocg.etherd.topology.Stage

private[etherd] object RuntimeMessages {

  // Client --> ClusterManager
  case class SubmitStages(topologyName: String, stages: List[Stage]) extends Serializable

  //ClusterManager --> TopologyExecutionManager
  case class ScheduleStages(stages: List[Stage])

  //* --> Scheduler
  case class ScheduleTasks(tasks: Iterator[SchedulableTask[_]]) extends Serializable

  // TopologyExecutionManager --> Executor
  case class Report(topologyName: String)
  case class ExecuteStage(stage: Stage)

  // Executor --> ExecutorWroker
  case class RunStage()

  // Executor --> TopologyExecutionManager
  case class ExecutorData(executorId: String, stageId: Int, partition: Int, host: String, port: Int, executorUrl: Option[String]=None) extends Serializable

  case class RegisterExecutor(topologyName: String, executorData: ExecutorData) extends Serializable

  case class ExecutorHeartBeat(executorId: String) extends Serializable

  case class StageExecutionScheduled(executorId: String, topologyName: String, stageId:Int)

  //Test Hooks
  case class GetRegisteredExecutors(topologyName: String) extends Serializable
  case class ExecutorList(executors: List[ExecutorData]) extends Serializable
  case class ShutdownAllScheduledTasks() extends Serializable
}
