package com.ocg.etherd.topology

import java.util.concurrent.atomic.AtomicInteger

import com.ocg.etherd.{ActorUtils, EtherdEnv}
import com.ocg.etherd.runtime.TopologyExecutionManager
import com.ocg.etherd.runtime.RuntimeMessages.SubmitStages
import scala.collection.mutable
import com.ocg.etherd.streams.{ReadableEventStream, WriteableEventStream}
import com.ocg.etherd.spn.{PassThroughSPN, SPN}

/**
 *
 */
class Topology(topologyName: String) {
  var topologySubmitted = false
  val env = EtherdEnv.get
  val ingestSpn = new PassThroughSPN(this.topologyName)

  def ingest(istream: ReadableEventStream) : SPN = {
    this.ingestSpn.attachInputStream(istream)
    this.ingestSpn
  }

  def ingest(istreams: Iterator[ReadableEventStream]): SPN = {
    istreams.foreach { _ => this.ingestSpn.attachInputStream(_) }
    this.ingestSpn
  }

  def run(): Unit = {
    if (topologySubmitted)
      return

    topologySubmitted = true
    val stages = {
      val stageList = mutable.ListBuffer.empty[Stage]
      this.ingestSpn.buildStages(stageList)
      stageList
    }
  }
}


