package com.ocg.etherd.topology

import java.util.concurrent.atomic.AtomicInteger

import com.ocg.etherd.{ActorUtils, EtherdEnv}
import com.ocg.etherd.runtime.RuntimeMessages.SubmitStages
import scala.collection.mutable
import com.ocg.etherd.streams.{ReadableEventStreamSpec, WritableEventStreamSpec}
import com.ocg.etherd.spn.{Ingest, SPN}

/**
 * A Topology is a specification of how streams and processing nodes should be composed to process events
 * It exposes user facing API's to express computation that will eventually run on a cluster
 * by the scheduler.
 * Topologies can be further composed together to form higher-order topologies. When a topology is run, internally:
 * 1. Build stages
 * 2. Setup event listeners for tracking topology execution
 * 3. Submit the stages to the cluster manager for execution
 */
class Topology(topologyName: String) {
  var topologySubmitted = false
  val env = EtherdEnv.get
  val ingestSpn = new Ingest(this.topologyName)

  def ingest(istreamSpec: ReadableEventStreamSpec) : SPN = {
    this.ingestSpn.attachInputStreamSpec(istreamSpec)
    this.ingestSpn
  }

  def ingest(istreams: Iterator[ReadableEventStreamSpec]): SPN = {
    istreams.foreach { _ => this.ingestSpn.attachInputStreamSpec(_) }
    this.ingestSpn
  }

  def run(): Unit = {
    if (topologySubmitted)
      return

    topologySubmitted = true
    val stages = {
      val stageList = mutable.ListBuffer.empty[Stage]
      this.ingestSpn.buildStages(stageList)
      stageList.toList
    }
    println("Number of stages is:" + stages.size)
    this.env.getClusterManagerRef ! SubmitStages("topology", stages)
  }
}

object Topology {
  def apply(name: String): Topology = {
    new Topology(name)
  }
}


