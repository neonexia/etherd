package com.ocg.etherd.spn

import com.ocg.etherd.streams.Event
import com.ocg.etherd.topology.StageExecutionContext

/**
 */
class MappedSPN(ec: StageExecutionContext, f: Event => Event ) extends SPN(ec) {
  override def processEvent(topic: String, event: Event): Unit = {
    this.linkOrSinkDefault(topic, f(event))
  }
}

class FlatMappedSPN(ec: StageExecutionContext, f: Event => Iterator[Event] ) extends SPN(ec) {
  override def processEvent(topic: String, event: Event): Unit = {
    this.linkOrSinkDefault(topic, f(event))
  }
}
