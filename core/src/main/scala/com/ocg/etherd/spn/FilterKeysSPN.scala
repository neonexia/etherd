package com.ocg.etherd.spn

import com.ocg.etherd.streams.{Event, WriteableEventStream}
import com.ocg.etherd.topology.SPNExecutionContext

/**
 *
 * @param ec
 * @param keys
 */
class FilterKeysSPN(ec: SPNExecutionContext, keys: List[String]) extends SPN(ec){

  override def processEvent(topic: String, event: Event ): Unit = {
    if (!this.filter(event)) {
      this.linkOrSinkDefault(topic, event)
    }
  }

  def filter(ev: Event): Boolean = {
    keys.contains(Event.keyAsString(ev))
  }
}
