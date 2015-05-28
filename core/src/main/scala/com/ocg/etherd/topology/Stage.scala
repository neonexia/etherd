package com.ocg.etherd.topology

import com.ocg.etherd.runtime.scheduler.{ResourceAsk, SchedulableTask}
import com.ocg.etherd.spn.SPN

trait PartitioningPolicy {
  var defaultPartitionSize = 1

  def setDefaultPartitionSize(partitions: Int) = this.defaultPartitionSize = Math.max(1, partitions)

  def getDefaultPartitionSize = this.defaultPartitionSize

  def applyPolicy: List[(Int, ResourceAsk)] = {
    // default policy is to use the defaultPartitionSize set
    (0 to this.defaultPartitionSize - 1).map { x => (x, new ResourceAsk(1,1))}.toList
  }
}

private[etherd] class Stage(spn: SPN) extends PartitioningPolicy with Serializable {
  var stageId: Option[Int] = None
  var topologyId: Option[String] = None
  var topologyExecutionManagerActorUrl: Option[String] = None

  def underlying = this.spn

  def setStageId(id: Int) = stageId = Some(id)

  def getStageId: Option[Int] = this.stageId

  def setTopologyId(id: String) = this.topologyId = Some(id)

  def setTopologyExecutionManagerActorUrl(topologyExecutionManagerActorUrl: String) =
    this.topologyExecutionManagerActorUrl = Some(topologyExecutionManagerActorUrl)

  def getTopologyExecutionManagerActorUrl = this.topologyExecutionManagerActorUrl

  def getTopologyId = this.topologyId

  def buildTasks: Iterator[SchedulableTask[StageSchedulingInfo]] = {
    this.applyPolicy.map { case (partition, resourceAsk) =>
      val schedulingInfo = StageSchedulingInfo(this.getStageId.get, partition,
        this.getTopologyId.get, this.getTopologyExecutionManagerActorUrl.get)
      new SchedulableTask[StageSchedulingInfo](schedulingInfo, resourceAsk)
    }.iterator
  }
}

case class StageSchedulingInfo(stageId: Int, partition:Int, topologyId: String, topologyExecutionManagerActorUrl: String)


