package com.ocg.etherd.topology

import scala.collection.mutable
import com.ocg.etherd.streams.{WriteableEventStream, ReadableEventStream}
import com.ocg.etherd.spn.{PassThroughSPN, SPN}

/**
 *

 */
class Topology(fqn: String) {
  val stageList = mutable.ListBuffer.empty[SPN]
  val ec: StageExecutionContext = new StageExecutionContext()
  def ingest(istreams: mutable.ListBuffer[ReadableEventStream], ostream: WriteableEventStream): SPN = {
    val spn = new PassThroughSPN(ec)
    istreams.foreach {_ => spn.attachInputStream(_) }
    spn
  }
}

class Stage(spn: SPN) {

}
