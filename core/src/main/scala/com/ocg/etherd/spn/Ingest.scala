package com.ocg.etherd.spn

import com.ocg.etherd.EtherdEnv
import com.ocg.etherd.streams._

/**
 * The processing node in the topology. Just passes the events through
 */
class Ingest(topologyName: String, delay:Int = 0, id: Int=SPN.newId()) extends SPN(id, topologyName) {

  override def processEvent(topic: String, event: Event): Unit = {
    if (delay > 0) {
      Thread.sleep(delay)
    }
    //println("Ingest. linkorsink")
    this.emit(topic, event)
   }
}
