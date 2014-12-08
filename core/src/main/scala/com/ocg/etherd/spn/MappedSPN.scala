package com.ocg.etherd.spn

import com.ocg.etherd.streams.Event
import com.ocg.etherd.topology.SPNExecutionContext

/**
 */
class MappedSPN(ec: SPNExecutionContext, f: Event => Event ) extends SPN(ec) {
  override def processEvent(topic: String, event: Event): Unit = {
    this.linkOrSinkDefault(topic, f(event))
  }
}

class FlatMappedSPN(ec: SPNExecutionContext, f: Event => Iterator[Event] ) extends SPN(ec) {
  override def processEvent(topic: String, event: Event): Unit = {
    this.linkOrSinkDefault(topic, f(event))
  }
}
