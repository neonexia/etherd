package com.ocg.etherd.spn

import com.ocg.etherd.EtherdEnv
import com.ocg.etherd.streams._

/**
 * Pass through
 */
class PassThroughSPN(topologyName: String, delay:Int = 0) extends SPN(SPN.newId(), topologyName) {

  override def processEvent(topic: String, event: Event): Unit = {
    if (delay > 0) {
      Thread.sleep(delay)
    }
    //println("PassThroughSPN. linkorsink")
    this.linkOrSinkDefault(topic, event)
   }
}
