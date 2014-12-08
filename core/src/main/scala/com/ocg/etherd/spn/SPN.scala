package com.ocg.etherd.spn

import com.ocg.etherd.topology.{Stage, SPNExecutionContext}
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
abstract class SPN(ec: SPNExecutionContext) {

  var linkedSpn: Option[SPN] = None
  var sinkedSPNs: mutable.ListBuffer[SPN] = mutable.ListBuffer.empty[SPN]

  def getExecutionContext = this.ec

  def processEvent(topic: String, event: Event): Unit

  final def beginProcessStreams(): Unit = {
    ec.getInputStreams.foreach { stream => {
      stream.subscribe((topic: String, event: Event) => {
        processEvent(topic, event)
        true
      })
    }
    }
  }

  def setLinkedSPN(spn: SPN): Unit = this.linkedSpn = Some(spn)

  def getLinkedSPN: Option[SPN] = this.linkedSpn

  def linkOrSinkDefault(topic: String, event: Event) = {
    this.getLinkedSPN match {
      case Some(spn) => spn.processEvent(topic, event)
      case None => this.ec.defaultOutStream.push(event)
    }
  }

  def linkOrSinkDefault(topic: String, events: Iterator[Event]) = {
    this.getLinkedSPN match {
      case Some(linkedSPN) => events.foreach { event => linkedSPN.processEvent(topic, event) }
      case None => this.ec.defaultOutStream.push(events)
    }
  }

  def addSinkedSPN(spn: SPN): Unit = {
    this.sinkedSPNs += spn
  }

  def buildStages(finalStageList: mutable.ListBuffer[Stage]): Unit = {
    finalStageList += this.ec.getNewStage(this)
    this.sinkedSPNs.foreach { _.buildStages(finalStageList) }
  }

  def map(func: Event => Event): SPN = {
    SPN.mappedSPN(this.ec, func)
  }

  def filterByKeys(keys: List[String]): SPN = {
    SPN.filter(this.ec, keys)
  }

  def filter(func: Event => Boolean): SPN = {
    SPN.filter(this.ec, func)
  }

  def sink(targets: List[SPN]) = {
    targets.foreach {this.sinkedSPNs += _}
  }
}

object SPN {
  def pass(ec: SPNExecutionContext): SPN = {
      new PassThroughSPN(ec, 100)
  }

  def filter(ec: SPNExecutionContext, keys: List[String]): SPN = {
    new FilterKeysSPN(ec, keys)
  }

  def filter(ec: SPNExecutionContext, func: Event=>Boolean): SPN = {
    //new Filter(ec, func)
    null
  }

  def mappedSPN(ec: SPNExecutionContext, func: Event => Event): SPN = {
    new MappedSPN(ec, func)
  }
}