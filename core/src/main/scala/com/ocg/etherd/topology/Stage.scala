package com.ocg.etherd.topology

import com.ocg.etherd.runtime.scheduler.{ResourceAsk, SchedulableTask}
import com.ocg.etherd.spn.SPN

class Stage(spn: SPN) {
  var stageId: Option[Int] = None
  var topologyId: Option[String] = None
  var topologyExecutionManagerActorUrl: Option[String] = None

  def setStageId(id: Int) = stageId = Some(id)

  def getStageId: Option[Int] = this.stageId

  def setTopologyId(id: String) = this.topologyId = Some(id)

  def setTopologyExecutionManagerActorUrl(topologyExecutionManagerActorUrl: String) = this.topologyExecutionManagerActorUrl = Some(topologyExecutionManagerActorUrl)

  def getTopologyExecutionManagerActorUrl = this.topologyExecutionManagerActorUrl

  def getTopologyId = this.topologyId

  def getTasks: Iterator[SchedulableTask[Stage]] = {
     List(new SchedulableTask[Stage](this, new ResourceAsk(1,1))).iterator
  }
}

class StageExecutionContext() {
  def waitForCompletion() = {
    this.wait()
  }

  def signalComplete() = {
    this.notifyAll()
  }
}


