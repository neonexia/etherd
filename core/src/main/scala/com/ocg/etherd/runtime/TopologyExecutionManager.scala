package com.ocg.etherd.runtime

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Terminated
import akka.event.Logging
import com.ocg.etherd.runtime.akkautils._
import com.ocg.etherd.EtherdEnv
import com.ocg.etherd.runtime.RuntimeMessages._
import com.ocg.etherd.topology.Stage

import scala.collection.mutable

class TopologyExecutionManager(topologyName: String, topologyExecutionManagerActorUrl: String) extends Actor {
  val log = Logging(context.system, this)
  val env = EtherdEnv.get
  val scheduler = this.env.getScheduler
  val stageIdInc = new AtomicInteger(1)
  val stageIdExecutorsMap = mutable.Map.empty[Int, ExecutorData]
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
      def watchExecutor() = {
        val actorRef = Utils.resolveActor(this.context.system, executorData.executorUrl.get)
        this.context.watch(actorRef)
      }
      watchExecutor()

      // register stageId -> ExecutorData
      val stageId = executorData.stageId
      this.stageIdExecutorsMap += stageId -> executorData

      // send the stage to the executor
      log.info(s"Sending stage:$stageId to executor:$executorId")
      val stage = this.stageIdStageMap.get(stageId).get
      sender ! RuntimeMessages.ExecuteStage(stage)
    }

    case GetRegisteredExecutors(topologyName: String) => {
      sender ! ExecutorList(this.stageIdExecutorsMap.values.toList)
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
