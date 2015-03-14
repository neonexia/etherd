package com.ocg.etherd.spn

import com.ocg.etherd.EtherdEnv
import com.ocg.etherd.streams.Event


/**
 */
class MapSPN(topologyName: String, f: Event => Event ) extends SPN(SPN.newId(), topologyName) {
  override def processEvent(topic: String, event: Event): Unit = {
    this.linkOrSinkDefault(topic, f(event))
  }
}

class FlatMapSPN(topologyName: String, f: Event => Iterator[Event] ) extends SPN(SPN.newId(), topologyName) {
  override def processEvent(topic: String, event: Event): Unit = {
    this.linkOrSinkDefault(topic, f(event))
  }
}
