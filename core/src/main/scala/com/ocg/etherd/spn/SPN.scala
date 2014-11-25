package com.ocg.etherd.spn

import com.ocg.etherd.streams.WriteableEventStream
import com.ocg.etherd.topology.TopologyContext
import com.ocg.etherd.topology.StageContext
import com.ocg.etherd.streams._

/**
 * SPN's model ether's event stream processing nodes. Each SPN is like a query operator
 * eg: JoinSPN, FilterSPN, MapSPN, AggregateSPN(Average, Sum) etc. Each SPN can have 1+ input event streams
 * and a 1 output event streams. Each SPN wraps:
 * Toplogy Context --> TopologyContext
 * Input Streams --> Set[EventStream]
 * Output Stream --> EventStream
 * StageId --> String
 * PartitionId (within that stage id) --> Int
 */
abstract class SPN(tc: TopologyContext) {

  def getTopologyContext = this.tc

  def processEvent(topic: String, event: Event, ostream: WriteableEventStream)

  def beginProcessStreams(): Unit = {
      tc.getInputStreams.foreach { stream => {
        stream.subscribe((topic: String, event: Event) => {
          this.processEvent(topic, event, this.tc.defaultOutStream)
          true
        })
      }
    }
  }
}

object SPN {
  def pass(tc: TopologyContext): SPN = {
      new PassThroughSPN(tc, 100)
  }

  def filter(tc: TopologyContext, func: (Event) => Boolean): SPN = {
    new FilterSPN(tc, func)
  }
}