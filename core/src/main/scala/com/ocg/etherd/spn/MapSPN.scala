package com.ocg.etherd.spn

import com.ocg.etherd.EtherdEnv
import com.ocg.etherd.streams.Event


/**
 */
class MapSPN(topologyName: String, mapper: Event => Event, id:Int = SPN.newId()) extends SPN(id, topologyName) {
  override def processEvent(topic: String, event: Event): Unit = {
    this.linkOrSinkDefault(topic, mapper(event))
  }
}

class FlatMapSPN(topologyName: String, mapper: Event => Iterator[Event], id:Int = SPN.newId() ) extends SPN(id, topologyName) {
  override def processEvent(topic: String, event: Event): Unit = {
    this.linkOrSinkDefault(topic, mapper(event))
  }
}
