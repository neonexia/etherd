package com.ocg.etherd.spn

import com.ocg.etherd.topology.{Stage, StageExecutionContext}
import com.ocg.etherd.streams._
import scala.collection.mutable

/**
 * SPN's model ether's event stream processing nodes. Each SPN is like a query operator
 * eg: JoinSPN, FilterSPN, MapSPN, AggregateSPN(Average, Sum) etc. Each SPN can have 1+ input event streams
 * and a 1 output event streams. Each SPN has knowledge of:
 * Execution Context --> The execution context that
 * Input Streams --> Set[EventStream]
 * Output Stream --> EventStream
 * StageId --> String
 * PartitionId (within that stage id) --> Int
 */
abstract class SPN(ec: StageExecutionContext) {
  var linkedSpn: Option[SPN] = None
  var sinkedSPNs: mutable.ListBuffer[SPN] = mutable.ListBuffer.empty[SPN]
  var istreams: mutable.ListBuffer[ReadableEventStream] = mutable.ListBuffer.empty[ReadableEventStream]
  var defaultOutStream: WriteableEventStream = _

  def processEvent(topic: String, event: Event): Unit

  final def beginProcessStreams(): Unit = {
    this.istreams.foreach { stream => {
      stream.subscribe((topic: String, event: Event) => {
        processEvent(topic, event)
        true
      })
    }
    }
  }

  def attachInputStream(stream: ReadableEventStream) = {
    this.istreams += stream
  }

  def setdefaultOutputStream(stream: WriteableEventStream) = {
    this.defaultOutStream = stream
  }

  def addSinkedSPN(spn: SPN): Unit = {
    this.sinkedSPNs += spn
  }

  def setLinkedSPN(spn: SPN): Unit = {
    this.linkedSpn = Some(spn)
  }

  def getLinkedSPN: Option[SPN] = this.linkedSpn

  def linkOrSinkDefault(topic: String, event: Event) = {
    this.getLinkedSPN match {
      case Some(spn) => spn.processEvent(topic, event)
      case None => this.defaultOutStream.push(event)
    }
  }

  def linkOrSinkDefault(topic: String, events: Iterator[Event]) = {
    this.getLinkedSPN match {
      case Some(linkedSPN) => events.foreach { event => linkedSPN.processEvent(topic, event) }
      case None => this.defaultOutStream.push(events)
    }
  }

  def buildStages(finalStageList: mutable.ListBuffer[Stage]): Unit = {
    finalStageList += this.ec.getNewStage(this)
    this.sinkedSPNs.foreach { _.buildStages(finalStageList) }
  }

  def map(func: Event => Event): SPN = {
    val mappedSpn = new MappedSPN(this.ec, func)
    this.istreams.foreach { _ => mappedSpn.attachInputStream(_) }
    mappedSpn.setdefaultOutputStream(this.defaultOutStream)
    this.setLinkedSPN(mappedSpn)
    mappedSpn
  }

  def filterByKeys(keys: List[String]): SPN = {
    val filterSpn = new FilterKeysSPN(ec, keys)
    this.istreams.foreach { _ => filterSpn.attachInputStream(_) }
    filterSpn.setdefaultOutputStream(this.defaultOutStream)
    this.setLinkedSPN(filterSpn)
    filterSpn
  }

//  def filter(func: Event => Boolean): SPN = {
//    val filterSpn = SPN.filter(this.ec, func)
//    this.istreams.foreach { _ => filterSpn.attachInputStream(_) }
//    filterSpn.setdefaultOutputStream(this.defaultOutStream)
//    filterSpn
//  }

  def sink(targets: List[SPN]) = {
    targets.foreach { target =>
      this.sinkedSPNs += target
    }
  }
}