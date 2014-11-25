package com.ocg.etherd.spn

import com.ocg.etherd.streams._
import com.ocg.etherd.topology.TopologyContext

/**
 * Pass through
 * @param tc
 */
class PassThroughSPN(tc: TopologyContext, delay:Int = 0) extends SPN(tc) {

  override def processEvent(topic: String, event: Event, ostream: WriteableEventStream): Unit = {
    ostream.push(event)
    if (delay > 0) {
      Thread.sleep(delay)
    }
  }
}
