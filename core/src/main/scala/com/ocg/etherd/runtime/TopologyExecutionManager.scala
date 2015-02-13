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


  def scheduleStages(stages: List[Stage]) = {
    stages.foreach { stage => {
      val stageId = this.stageIdInc.getAndIncrement
      stage.setStageId(stageId)
      stage.setTopologyId(topologyName)
      stage.setTopologyExecutionManagerActorUrl(topologyExecutionManagerActorUrl)
      log.info(s"Submitting Stage $stageId to scheduler. Topology: $topologyName")
      this.env.getScheduler.submit(stage.getTasks)
    }
    }
  }

  def receive = {

    case ScheduleStages(stages: List[Stage]) => {
      this.scheduleStages(stages)
    }

    case RegisterExecutor(topologyName: String, executorData: ExecutorData)  => synchronized {
      val executorid = executorData.executorId
      log.info(s"Received Executor registration: $executorid for topology $topologyName")
      this.topologyExecutors += executorData
    }
    case GetRegisteredExecutors(topologyName: String) => {
      sender ! ExecutorList(this.topologyExecutors.toList)
    }

    case _ => log.info("Unknown Message received")
  }
}

object TopologyExecutionManager {

  def apply(topologyName: String, topologyExecutionManagerActorUrl: String): TopologyExecutionManager = {
    new TopologyExecutionManager(topologyName, topologyExecutionManagerActorUrl)
  }
}
