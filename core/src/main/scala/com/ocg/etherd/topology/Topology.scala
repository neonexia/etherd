package com.ocg.etherd.topology

import scala.collection.mutable
import com.ocg.etherd.{Logging, EtherdEnv}
import com.ocg.etherd.spn.{SPN, EventOps}
import com.ocg.etherd.runtime.RuntimeMessages.SubmitStages
import com.ocg.etherd.streams.{ReadableStreamSpec, EventStreamSpec}

/**
 * A Topology is a specification of how streams and processing nodes will be composed to process events.
 * Topologies can be further composed together to form more complex topologies.
 * The class will be the primary enrty for user facing APIs.
 * When a topology is run the internal flow is to create a topology runnable instance that will:
 * 1. Compile the specification into a stages and optionally run it through an optimizer(Optimizer is furture work)
 * 2. Setup event listeners for tracking topology execution
 * 3. Submit the stages to the cluster manager for execution
 * 4. Cluster Manager calls te schduler to allocate resources
 * 5. Execute stages as tasks on those resources
 * @param topologyName
 * @param defaultPartitions
 */
class Topology(topologyName: String, defaultPartitions: Int = 1 /*?? from constants */)
  extends Runnable with Logging  {
  var topologySubmitted = false
  val env = EtherdEnv.get
  val ingestSpn = EventOps.pass(this.topologyName)

  def ingest(topic: String) : SPN = {
    this.ingest(new ReadableStreamSpec(topic))
  }

  def ingest(istreamSpec: EventStreamSpec) : SPN = {
    this.ingestSpn.attachInputStreamSpec(istreamSpec)
    this.ingestSpn
  }

  def ingest(istreams: Iterator[EventStreamSpec]): SPN = {
    istreams.foreach { _ => this.ingestSpn.attachInputStreamSpec(_) }
    this.ingestSpn
  }

  override def run(): Unit = {
    if (!topologySubmitted) {
      topologySubmitted = true

      val stages = {
        val stageList = mutable.ListBuffer.empty[Stage]
        this.ingestSpn.buildStages(stageList)
        stageList.toList
      }
      logInfo(s"Number of stages for topology: $topologyName: " + stages.size)

      stages.foreach { stage => stage.setDefaultPartitionSize(defaultPartitions) }
      this.env.getClusterManagerRef ! SubmitStages(this.topologyName, stages)
    }
  }
}

object Topology {
  def apply(name: String, defaultPartitions: Int = 1): Topology = {
    new Topology(name, defaultPartitions)
  }
}
