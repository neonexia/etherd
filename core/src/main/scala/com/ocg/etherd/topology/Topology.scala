package com.ocg.etherd.topology
import scala.collection.mutable
import com.ocg.etherd.spn.{PassThroughSPN, SPN}
/**
 *
 */
class Topology(fqn: String) {
  val stageList = mutable.ListBuffer.empty[SPN]
  val ec: StageExecutionContext = new StageExecutionContext()
  def ingest(): SPN = {
    new PassThroughSPN(ec)
  }
}

class Stage(spn: SPN) {

}
