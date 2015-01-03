package com.ocg.etherd.topology

import java.util.concurrent.atomic.AtomicInteger

import com.ocg.etherd.scheduler.{LocalScheduler, Scheduler}
import scala.collection.mutable
import com.ocg.etherd.messaging.LocalDMessageBus
import com.ocg.etherd.streams.{ReadableEventStream, WriteableEventStream}
import com.ocg.etherd.spn.{PassThroughSPN, SPN}

/**
 *
 */
class Topology(fqn: String) {
  val stageIdInc = new AtomicInteger(1)
  var topologySubmitted = false
  val env: EtherdEnv = this.buildEtherdEnv
  val ingestSpn = new PassThroughSPN(this.env)

  def ingest(istreams: Iterator[ReadableEventStream]): SPN = {
    istreams.foreach { _ => this.ingestSpn.attachInputStream(_) }
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
          this.env.getScheduler.submit(stage.getTasks)
        }
      }
    }
  }

  private def buildEtherdEnv = {
    new EtherdEnv(this.fqn)
  }
}

class EtherdEnv(topologyName: String) {
  val scheduler = this.getScheduler
  val messageBus = this.getMessageBus

  def getTopologyName = this.topologyName

  def getScheduler = new LocalScheduler(2, 2)

  def getMessageBus = new LocalDMessageBus()
}


