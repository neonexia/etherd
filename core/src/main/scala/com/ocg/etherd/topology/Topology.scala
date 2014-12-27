package com.ocg.etherd.topology

import java.util.concurrent.atomic.AtomicInteger

import com.ocg.etherd.messaging.LocalDMessageQueueStream
import com.ocg.etherd.scheduler.{LocalScheduler, Scheduler}

import scala.collection.mutable
import com.ocg.etherd.streams.{WriteableEventStream, ReadableEventStream}
import com.ocg.etherd.spn.{PassThroughSPN, SPN}

/**
 *
 */
class Topology(fqn: String) {
  val ec: StageExecutionContext = new StageExecutionContext()
  val ingestSpn: SPN = new PassThroughSPN(this.ec)
  val scheduler = this.getScheduler
  val stageIdInc = new AtomicInteger(1)
  var topologySubmitted = false

  def ingest(istreams: Iterator[ReadableEventStream]): SPN = {
    istreams.foreach { _ => this.ingestSpn.attachInputStream(_) }
    this.ingestSpn.defaultOutStream = this.getDefaultOutStream
    this.ingestSpn
  }

  def run(): Unit = {
    if (!topologySubmitted) {
      topologySubmitted = true
      val stages = {
        val stageList = mutable.ListBuffer.empty[Stage]
        this.ingestSpn.buildStages(stageList)
        stageList
      }
      stages.foreach { stage => {
        stage.getStageId match {
          case Some(value) => null
          case None => stage.setStageId(this.stageIdInc.getAndIncrement)
        }
        stage.setTopologyId(this.fqn)

        this.scheduler.submit(stage.getTasks)
      }
      }
    }
  }

  private def getScheduler = new LocalScheduler(2)

  private def getDefaultOutStream = new LocalDMessageQueueStream(this.fqn + "_out")
}
