package com.ocg.etherd.spn

import com.ocg.etherd.streams.Event

/**
 * Defines most common operation and types for events, SPN's should try to reuse these where they can
 */
private[etherd] object EventOps {

  type EventMapOp = Event => Event

  type EventFlatMapOp = Event => Iterator[Event]

  type EventSelectOp = Event => Boolean

  /**
   * Event passthrough
   * @param topologyName
   * @param delay
   * @param id
   * @return
   */
  def pass(topologyName: String, delay:Int = 0, id:Int = SPN.newId()) = new SPN(id, topologyName) {
    override def processEvent(topic: String, event: Event): Unit = {
      if (delay > 0) {
        Thread.sleep(delay)
      }
      this.emit(topic, event)
    }
  }
  def map(topologyName: String, mapOp: EventMapOp, id:Int = SPN.newId()) = new SPN(id, topologyName) {
    override def processEvent(topic: String, event: Event): Unit = {
      this.emit(topic, mapOp(event))
    }
  }

  def flatMap(topologyName: String, flatMapOp: EventFlatMapOp, id:Int = SPN.newId()) = new SPN(id, topologyName) {
    override def processEvent(topic: String, event: Event): Unit = {
      this.emit(topic, flatMapOp(event))
    }
  }

  def select(topologyName: String, selectOp:EventSelectOp, id: Int=SPN.newId()) = new SPN(id, topologyName) {
    override def processEvent(topic: String, event: Event ): Unit = {
      if (!selectOp(event)) {
        this.emit(topic, event)
      }
    }
  }

  def selectByKeys(topologyName: String, keys: List[String], id: Int=SPN.newId()) = new SPN(id, topologyName) {
    val selectOp:EventSelectOp = (ev:Event) => keys.contains(Event.keyAsString(ev))

    override def processEvent(topic: String, event: Event ): Unit = {
      if (this.selectOp(event)) {
        this.emit(topic, event)
      }
    }
  }

  def dropByKeys(topologyName: String, keys: List[String], id: Int=SPN.newId()) = new SPN(id, topologyName) {
    val selectOp:EventSelectOp = (ev:Event) => keys.contains(Event.keyAsString(ev))

    override def processEvent(topic: String, event: Event ): Unit = {
      if (!this.selectOp(event)) {
        this.emit(topic, event)
      }
    }
  }
}
