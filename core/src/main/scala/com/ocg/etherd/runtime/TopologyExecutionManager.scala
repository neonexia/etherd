package com.ocg.etherd.runtime

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Terminated
import akka.event.Logging
import com.ocg.etherd.runtime.akkautils._
import com.ocg.etherd.EtherdEnv
import com.ocg.etherd.runtime.RuntimeMessages._
import com.ocg.etherd.topology.Stage

/**
 * Manages the execution lifecycle and monitoring of stages in a topology.
 * This is a seperate child Actor than the cluster manager. This can be run in process on the same node as the cluster
 * manager or on a different node on the cluster or within a container managed by the resource manager itself eg: Yarn
 * @param topologyName
 * @param topologyExecutionManagerActorUrl
 */
class TopologyExecutionManager(topologyName: String, topologyExecutionManagerActorUrl: String) extends Actor {
  val log = Logging(context.system, this)
  val env = EtherdEnv.get
  val scheduler = this.env.getScheduler
  val stageIdInc = new AtomicInteger(1)
  val stageIdExecutorsMap = mutable.Map.empty[Int, ListBuffer[ExecutorData]]
  val stageIdStageMap = mutable.Map.empty[Int, Stage]
  var executionState = RuntimeState.Init

  def receive = {
    case ScheduleStages(stages: List[Stage]) => synchronized {
      assert(this.executionState != RuntimeState.Scheduled)
      this.executionState = RuntimeState.Scheduled

      def scheduleStages(stages: List[Stage]): Unit = {
        stages.foreach { stage => {
          val stageId = this.stageIdInc.getAndIncrement
          stage.setStageId(stageId)
          stage.setTopologyId(topologyName)
          stage.setTopologyExecutionManagerActorUrl(topologyExecutionManagerActorUrl)
          this.stageIdStageMap += stageId -> stage
          log.info(s"Submitting Stage $stageId to scheduler. Topology: $topologyName")

          this.env.getScheduler.submit(stage.buildTasks)
        }
        }
      }
      scheduleStages(stages)
    }

    case RegisterExecutor(topologyName: String, executorData: ExecutorData)  => synchronized {
      val executorId = executorData.executorId
      log.info(s"Received Executor registration: $executorId for topology $topologyName")

      //setup a deathwatch
      val actorRef = Utils.resolveActor(this.context.system, executorData.executorUrl.get)
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
      log.info(s"Sending stage:$stageId to executor:$executorId")
      val stage = this.stageIdStageMap.get(stageId).get
      sender ! RuntimeMessages.ExecuteStage(stage)
    }

    case GetRegisteredExecutors(topologyName: String) => {
      sender ! ExecutorList(this.stageIdExecutorsMap.values.flatten.toList)
    }

    case Terminated(executorActorRef:ActorRef) => {
      val executorActorPath = executorActorRef.path
      if (this.executionState == RuntimeState.Scheduled) {
        log.warning(s"Executor $executorActorPath terminated")
        // ?? restart the executor and reschedule the stages
      }
      else
        log.info(s"Executor $executorActorPath terminated")
    }
    case _ => log.error("Unknown Message received")
  }
}
