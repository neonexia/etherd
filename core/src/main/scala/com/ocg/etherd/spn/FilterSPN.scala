package com.ocg.etherd.spn

import com.ocg.etherd.streams.{Event, WriteableEventStream}
import com.ocg.etherd.topology.TopologyContext

/**
 *
 * @param tc
 */
class FilterSPN(tc: TopologyContext, func: (Event) => Boolean) extends SPN(tc) {

  override def processEvent(topic: String, event: Event, ostream: WriteableEventStream ): Unit = {
//    if (key.asInstanceOf[String] == "#iluvdata") {
//      ostream.push((key, record))
//    }
  }
}
