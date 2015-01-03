package com.ocg.etherd.spn

import com.ocg.etherd.streams.{Event, WriteableEventStream}
import com.ocg.etherd.topology.EtherdEnv

/**
 *
 * @param ec
 * @param keys
 */
class FilterKeysSPN(ec: EtherdEnv, keys: List[String]) extends SPN(ec, SPN.newId()){

  override def processEvent(topic: String, event: Event ): Unit = {
    if (!this.filter(event)) {
      this.linkOrSinkDefault(topic, event)
    }
  }

  def filter(ev: Event): Boolean = {
    keys.contains(Event.keyAsString(ev))
  }
}
