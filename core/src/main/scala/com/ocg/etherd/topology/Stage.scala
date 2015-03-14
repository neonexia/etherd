package com.ocg.etherd.topology

import com.ocg.etherd.runtime.scheduler.{ResourceAsk, SchedulableTask}
import com.ocg.etherd.spn.SPN

class Stage(spn: SPN) extends Serializable {
  var stageId: Option[Int] = None
  var topologyId: Option[String] = None
  var topologyExecutionManagerActorUrl: Option[String] = None

  def underlying = this.spn

  def setStageId(id: Int) = stageId = Some(id)

  def getStageId: Option[Int] = this.stageId

  def setTopologyId(id: String) = this.topologyId = Some(id)

  def setTopologyExecutionManagerActorUrl(topologyExecutionManagerActorUrl: String) = this.topologyExecutionManagerActorUrl = Some(topologyExecutionManagerActorUrl)

  def getTopologyExecutionManagerActorUrl = this.topologyExecutionManagerActorUrl

  def getTopologyId = this.topologyId

  def buildTasks: Iterator[SchedulableTask[StageSchedulingInfo]] = {
    val schedulingInfo = StageSchedulingInfo(this.getStageId.get, this.getTopologyId.get, this.getTopologyExecutionManagerActorUrl.get)
    val resourceAsk = new ResourceAsk(1, 1)
    List(new SchedulableTask[StageSchedulingInfo](schedulingInfo, resourceAsk)).iterator
  }
}

case class StageSchedulingInfo(stageId: Int, topologyId: String, topologyExecutionManagerActorUrl: String)


