package com.ocg.etherd.topology
import scala.collection.mutable
import com.ocg.etherd.spn.SPN
/**
 *
 */
class Topology(fqn: String) {
  val stageList = mutable.ListBuffer.empty[SPN]
}

class Stage(spn: SPN) {

}
