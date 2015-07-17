package com.ocg.etherd.runtime

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Terminated
import com.ocg.etherd.runtime.akkautils._
import com.ocg.etherd.{Logging, EtherdEnv}
import com.ocg.etherd.runtime.RuntimeMessages._
import com.ocg.etherd.topology.Stage

/**
 * Manages the execution lifecycle and monitoring of stages in a topology.
 * This is a seperate child Actor than the cluster manager. This can be run in process on the same node as the cluster
 * manager or on a different node on the cluster or within a container managed by the resource manager itself eg: Yarn
 * @param topologyName
 * @param topologyExecutionManagerActorUrl
 */
class TopologyExecutionManager(topologyName: String, topologyExecutionManagerActorUrl: String) extends Actor with Logging {
  val env = EtherdEnv.get
  val scheduler = this.env.getScheduler
  val stageIdInc = new AtomicInteger(1)
  val stageIdExecutorsMap = mutable.Map.empty[Int, ListBuffer[ExecutorData]]//multiple executors per stage (1 per partition)
  val stageIdStageMap = mutable.Map.empty[Int, Stage]
  val executorActorRefStageIdMap = mutable.Map.empty[ActorRef, Int]
  var executionState = RuntimeState.Init

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {

  }

  override def postStop(): Unit = {
    this.terminateAllExecutors("postStop")
  }

  def receive = {
    case ScheduleStages(stages: List[Stage]) => {
      // ?? at some point we enable rescheduling of failed stages
      assert(this.executionState != RuntimeState.Scheduled)
      this.executionState = RuntimeState.Scheduled

      // foreach stage build the tasks(1 task per stage partition) and schedule them
      // one executor per task
      stages.foreach { stage => {
          val stageId = this.stageIdInc.getAndIncrement
          stage.setStageId(stageId)
          stage.setTopologyId(topologyName)
          stage.setTopologyExecutionManagerActorUrl(topologyExecutionManagerActorUrl)
          this.stageIdStageMap += stageId -> stage
          logDebug(s"Scheduling stage: $stageId for topology: $topologyName")
          this.env.getScheduler.submit(stage.buildTasks)
        }
      }
    }

    case RegisterExecutor(topologyName: String, executorData: ExecutorData)  => {
      val executorId = executorData.executorId
      logInfo(s"Received Executor registration: $executorId for topology $topologyName")

      //setup a deathwatch
      val actorRef = Utils.resolveActor(this.context.system, executorData.executorUrl.get)
      this.executorActorRefStageIdMap.getOrElseUpdate(actorRef, executorData.stageId)
      this.context.watch(actorRef)

      // register stageId -> ExecutorData
      val stageId = executorData.stageId
      this.stageIdExecutorsMap.get(stageId) match {
        case Some(executorDataList) => executorDataList += executorData
        case None => {
          val executorDataList = ListBuffer[ExecutorData](executorData)
          stageIdExecutorsMap += stageId -> executorDataList
        }
      }

      // send the stage to the executor
      logDebug(s"Sending stage:$stageId to executor:$executorId")
      val stage = this.stageIdStageMap.get(stageId).get
      sender ! RuntimeMessages.ExecuteStage(stage)
    }

    case GetRegisteredExecutors(topologyName: String) => {
      sender ! ExecutorList(this.stageIdExecutorsMap.values.flatten.toList)
    }

    case Terminated(executorActorRef: ActorRef) => {
      val executorActorPath = executorActorRef.path
        logWarning(s"Executor $executorActorPath terminated")
    }

    case _ => logError("Unknown Message received")
  }

  private def terminateAllExecutors(shutdownReason: String): Unit = {
    // send shutdown signal to all executors
    stageIdExecutorsMap.foreach { case(stageId, listOfExecutors) => {
      logInfo(s"Asking executors to shutdown for stage $stageId")
      listOfExecutors.foreach { ex =>
        Utils.resolveActor(this.context.system, ex.executorUrl.get) ! new ControlledShutdown(shutdownReason)
      }
    }}
  }
}
