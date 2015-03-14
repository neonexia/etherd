package com.ocg.etherd.runtime

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.Actor
import akka.event.Logging
import com.ocg.etherd.EtherdEnv
import com.ocg.etherd.runtime.RuntimeMessages._
import com.ocg.etherd.topology.Stage

import scala.collection.mutable

class TopologyExecutionManager(topologyName: String,
                               topologyExecutionManagerActorUrl: String) extends Actor {
  val log = Logging(context.system, this)
  val env = EtherdEnv.get
  val scheduler = this.env.getScheduler
  val stageIdInc = new AtomicInteger(1)
  val topologyExecutors = mutable.ListBuffer.empty[ExecutorData]
  val stageIdStageMap = mutable.Map.empty[Int, Stage]
  var executionState = RuntimeState.Init

  def receive = {
    case ScheduleStages(stages: List[Stage]) => synchronized {
      assert(this.executionState != RuntimeState.Scheduled)
      this.executionState = RuntimeState.Scheduled
      this.scheduleStages(stages)
    }

    case RegisterExecutor(topologyName: String, executorData: ExecutorData)  => synchronized {
      val executorid = executorData.executorId
      log.info(s"Received Executor registration: $executorid for topology $topologyName")
      this.topologyExecutors += executorData
      // ship the stage to the executor
      def shipStage() = {
        log.info("Checking for stageid:  " + executorData.stageId.toString)
        this.stageIdStageMap.get(executorData.stageId) match {
          case Some(stage) => sender ! RuntimeMessages.ExecuteStage(stage)
          case None =>  {
            log.error("We should not be in this state")
            assert(false, "We should not be in this state")
          }
        }
      }

      shipStage()
    }

    case GetRegisteredExecutors(topologyName: String) => {
      sender ! ExecutorList(this.topologyExecutors.toList)
    }

    case _ => log.error("Unknown Message received")
  }

  private def scheduleStages(stages: List[Stage]): Unit = {
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
}
