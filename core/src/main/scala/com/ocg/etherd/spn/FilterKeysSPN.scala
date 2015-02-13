package com.ocg.etherd.spn

import com.ocg.etherd.EtherdEnv
import com.ocg.etherd.streams.Event

/**
 *
 */
class FilterKeysSPN(topologyName: String, keys: List[String]) extends SPN(SPN.newId(), topologyName){

  override def processEvent(topic: String, event: Event ): Unit = {
    if (!this.filter(event)) {
      this.linkOrSinkDefault(topic, event)
    }
  }

  def filter(ev: Event): Boolean = {
    keys.contains(Event.keyAsString(ev))
  }
}
