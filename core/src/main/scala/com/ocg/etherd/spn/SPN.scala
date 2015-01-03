package com.ocg.etherd.spn

import java.util.concurrent.atomic.AtomicInteger

import com.ocg.etherd.topology.{Topology, Stage, EtherdEnv}
import com.ocg.etherd.streams._
import scala.collection.mutable

/**
 * Each SPN defines some operation on the incoming streams. Its similar to a query operator.
 * eg: JoinSPN, FilterSPN, MapSPN, AggregateSPN(Average, Sum) etc.
 * Each SPN can have 1+ input streams and at least one output stream.
 */
abstract class SPN(env: EtherdEnv, spnId: Int) {
  private var linkedSpn: Option[SPN] = None
  private var sinkedSPNs: mutable.ListBuffer[SPN] = mutable.ListBuffer.empty[SPN]
  private var istreams: mutable.ListBuffer[ReadableEventStream] = mutable.ListBuffer.empty[ReadableEventStream]
  private var externalOstreams: mutable.ListBuffer[WriteableEventStream] = mutable.ListBuffer.empty[WriteableEventStream]
  private var defaultOutStream: Option[WriteableEventStream] = None

  def getId = this.spnId

  def processEvent(topic: String, event: Event): Unit

  /**
   * This is called by the execution engine on the cluster node when the topology begins executing
   * Tasks here are
   *   -initialize and subscribe to all input streams
   *   -- initialize output streams
   * The partition
   * @param partition
   */
  final def beginProcessStreams(partition: Int = 0): Unit = {
    // println(this.getId.toString)
    this.defaultOutStream match {
      case Some(stream) => {
        // println("in beginProcessStreams. Calling init for defaultOutstream: " + stream.topic.toString)
        stream.init(partition)
      }
      case _ => println("Nothing to do")
    }

    this.externalOstreams.foreach { stream => stream.init(partition)}

    this.istreams.foreach { stream => {
      // println("in beginProcessStreams. Calling init for istream: " + stream.topic.toString)
      stream.subscribe((topic: String, event: Event) => {
        processEvent(topic, event)
        true
      })
      stream.init(partition)
    }
    }
  }

  protected def linkOrSinkDefault(topic: String, event: Event) = {
    this.getLinkedSPN match {
      case Some(spn) => spn.processEvent(topic, event)
      case None => this.defaultOutStream match {
        case Some(ostream) => ostream.push(event)
        case None => println("Dropping event since no sinked spn or outstream")
      }
    }
    // push to all external output streams
    this.externalOstreams.foreach { ostream => {
      ostream.push(event)
    }}
  }

  protected def linkOrSinkDefault(topic: String, events: Iterator[Event]) = {
    this.getLinkedSPN match {
      case Some(linkedSPN) => events.foreach { event => linkedSPN.processEvent(topic, event) }
      case None => this.defaultOutStream match {
        case Some(ostream) => ostream.push(events)
        case None => println("Dropping events since no sinked spn or outstream")
      }
    }

    // push to all external output streams
    this.externalOstreams.foreach { ostream => {
      ostream.push(events)
    }}
  }

  def attachInputStream(stream: ReadableEventStream) = {
    this.istreams += stream
  }

  def attachInputStreams(streams: ReadableEventStream*) = {
    streams.foreach { istream => this.attachInputStream(istream)}
  }

  def attachExternalOutputStream(stream: WriteableEventStream) = {
    this.externalOstreams += stream
  }

  def attachExternalOutputStreams(streams: WriteableEventStream*) = {
    streams.foreach { ostream => this.attachExternalOutputStream(ostream) }
  }

  def getDefaultOutputStream = {
    val ostream = this.defaultOutStream match {
      case Some(stream) => stream
      case None => {
        val wstream = this.env.messageBus.buildWriteOnlyStream(this.env.getTopologyName + this.getId.toString)
        this.defaultOutStream = Some(wstream)
        wstream
      }
    }
    Some(ostream)
  }

  def setdefaultOutputStream(stream: WriteableEventStream) = {
    // println("setting default out stream:" +  stream.get.topic + "on spn: " + this.getId)
    this.defaultOutStream = Some(stream)
  }

  def buildStages(finalStageList: mutable.ListBuffer[Stage]): Unit = {
    finalStageList += new Stage(this)
    this.sinkedSPNs.foreach { _.buildStages(finalStageList) }
  }

  def map(func: Event => Event): SPN = {
    val mappedSpn = new MappedSPN(this.env, func)
    mappedSpn.setdefaultOutputStream(this.getDefaultOutputStream.get)
    this.setLinkedSPN(mappedSpn)
    mappedSpn
  }

  def filterByKeys(keys: List[String]): SPN = {
    val filterSpn = new FilterKeysSPN(env, keys)
    filterSpn.setdefaultOutputStream(this.getDefaultOutputStream.get)
    this.setLinkedSPN(filterSpn)
    filterSpn
  }

  def sink(targets: List[SPN]) = {
    targets.foreach { target =>
      target.attachInputStream(this.env.messageBus.buildStream(this.getDefaultOutputStream.get.topic))
      this.sinkedSPNs += target
    }
  }

  private def getLinkedSPN: Option[SPN] = this.linkedSpn

  private def addSinkedSPN(spn: SPN): Unit = {
    this.sinkedSPNs += spn
  }

  private def setLinkedSPN(spn: SPN): Unit = {
    this.linkedSpn = Some(spn)
  }
}

object SPN {
  val spnIdInc = new AtomicInteger(1)

  def newId(): Int = {
    this.spnIdInc.getAndIncrement
  }
}