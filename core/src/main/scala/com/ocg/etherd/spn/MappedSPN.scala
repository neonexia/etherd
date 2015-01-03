package com.ocg.etherd.spn

import com.ocg.etherd.streams.Event
import com.ocg.etherd.topology.EtherdEnv

/**
 */
class MappedSPN(ec: EtherdEnv, f: Event => Event ) extends SPN(ec, SPN.newId()) {
  override def processEvent(topic: String, event: Event): Unit = {
    this.linkOrSinkDefault(topic, f(event))
  }
}

class FlatMappedSPN(ec: EtherdEnv, f: Event => Iterator[Event] ) extends SPN(ec, SPN.newId()) {
  override def processEvent(topic: String, event: Event): Unit = {
    this.linkOrSinkDefault(topic, f(event))
  }
}
