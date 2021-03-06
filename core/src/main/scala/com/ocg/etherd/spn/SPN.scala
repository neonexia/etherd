package com.ocg.etherd.spn

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable

import com.ocg.etherd.{Logging, EtherdEnv}
import com.ocg.etherd.topology.Stage
import com.ocg.etherd.streams._


/**
 * SPN defines some processing on the incoming streams.Each SPN can have 1+ input streams and
 * must have least one default output stream.
 */
abstract class SPN(spnId: Int, topologyName: String) extends Serializable with Logging{
  private var linkedSpn: Option[SPN] = None
  private var sinkedSPNs: mutable.ListBuffer[SPN] = mutable.ListBuffer.empty[SPN]
  private var istreamsSpec: mutable.ListBuffer[EventStreamSpec] = mutable.ListBuffer.empty[EventStreamSpec]
  private var istreams: mutable.ListBuffer[ReadableEventStream] = mutable.ListBuffer.empty[ReadableEventStream]
  private var externalOstreamsSpec: mutable.ListBuffer[EventStreamSpec] = mutable.ListBuffer.empty[EventStreamSpec]
  private var externalOstreams: mutable.ListBuffer[WritableEventStream] = mutable.ListBuffer.empty[WritableEventStream]
  private var defaultOutStreamSpec: Option[EventStreamSpec] = None
  private var defaultOutStream: Option[WritableEventStream] = None

  def getId = this.spnId

  def processEvent(topic: String, event: Event): Unit

  /**
   * Appends a new input stream specification to this node.
   * @param streamSpec
   * @return
   */
  def attachInputStreamSpec(streamSpec: EventStreamSpec) = {
    this.istreamsSpec += streamSpec
  }

  def attachInputStreamsSpec(streamSpecs: Iterator[EventStreamSpec]) = {
    streamSpecs.foreach { istreamSpec => this.attachInputStreamSpec(istreamSpec)}
  }

  /**
   * Appends a new external output stream specification to this SPN.
   * This SPN's(stage) output events will be routed to this stream
   * @param streamSpec
   */
  def attachExternalOutputStreamSpec(streamSpec: EventStreamSpec): Unit = {
    this.linkedSpn match {
      case Some(spn) => spn.attachExternalOutputStreamSpec(streamSpec)
      case None => this.externalOstreamsSpec += streamSpec
    }
  }

  def attachExternalOutputStreamSpecs(streamSpecs: Iterator[EventStreamSpec]): Unit = {
    this.linkedSpn match {
      case Some(spn) => spn.attachExternalOutputStreamSpecs(streamSpecs)
      case None => streamSpecs.foreach { ostreamSpec => this.attachExternalOutputStreamSpec(ostreamSpec)}
    }
  }

  def map(func: Event => Event): SPN = {
    val mapSpn = EventOps.map(this.topologyName, func, this.getId)
    this.setLinkedSPN(mapSpn)
    mapSpn
  }

  def flatMap(func: Event => Iterator[Event]): SPN = {
    val flatMap = EventOps.flatMap(this.topologyName, func, this.getId)
    this.setLinkedSPN(flatMap)
    flatMap
  }

  def selectByKeys(keys: List[String]): SPN = {
    val filterSpn = EventOps.selectByKeys(this.topologyName, keys, this.getId)
    this.setLinkedSPN(filterSpn)
    filterSpn
  }

  def dropByKeys(keys: List[String]): SPN = {
    val filterSpn = EventOps.dropByKeys(this.topologyName, keys, this.getId)
    this.setLinkedSPN(filterSpn)
    filterSpn
  }

  def split(targets: Iterator[SPN]): Unit = {
    this.linkedSpn match {
      case Some(spn) => spn.split(targets)
      case None => {
        targets.foreach { target =>
          this.sinkedSPNs += target
          target.attachInputStreamSpec(new ReadableStreamSpec(this.getOrBuildDefaultOutputStreamSpec.get.topic))
        }
      }
    }
  }

  /**
   * Sink output events to the target SPN.
   * Every sinked SPN will create a new stage in the processing pipeline
   * @param target
   * @return
   */
  def sink(target: SPN): SPN = {
    this.split(List(target).iterator)
    target
  }

  def sink(streamSpec: EventStreamSpec): SPN = {
    this.linkedSpn match {
      case Some(spn) => spn.sink(streamSpec)
      case None => this.externalOstreamsSpec += streamSpec
    }
    this
  }

  protected def emit(topic: String, event: Event) = {
    this.linkedSpn match {
      case Some(spn) => spn.processEvent(topic, event)
      case None => this.defaultOutStream.map { stream => stream.push(event) }
    }

    // push to all external output streams
    this.externalOstreams.foreach { ostream => {
      ostream.push(event)
    }}
  }

  protected def emit(topic: String, events: Iterator[Event]) = {
    this.linkedSpn match {
      case Some(linkedSPN) => events.foreach { event => linkedSPN.processEvent(topic, event) }
      case None => this.defaultOutStream.map { stream => stream.push(events) }
    }

    // push to all external output streams
    this.externalOstreams.foreach { ostream => ostream.push(events) }
  }

  private def buildLinkedStages(finalStageList: mutable.ListBuffer[Stage]): Unit = {
    this.linkedSpn match {
      case Some(spn) => spn.buildLinkedStages(finalStageList)
      case None => this.sinkedSPNs.foreach { _.buildStages(finalStageList) }
    }
  }

  private[etherd] def buildStages(finalStageList: mutable.ListBuffer[Stage]): Unit = {
    finalStageList += new Stage(this)
    this.buildLinkedStages(finalStageList)
  }

  private def getOrBuildDefaultOutputStreamSpec = {
    // if we have out stream spec return it else build it first
    val ostreamSpec = this.defaultOutStreamSpec match {
      case Some(streamSpec) => streamSpec
      case None => {
        val wstreamSpec = new WritableStreamSpec("$internal_" + this.topologyName + this.getId.toString)
        this.defaultOutStreamSpec = Some(wstreamSpec)
        wstreamSpec
      }
    }
    Some(ostreamSpec)
  }

  private def setdefaultOutputStreamSpec(streamSpec: EventStreamSpec) = {
    this.defaultOutStreamSpec = Some(streamSpec)
  }

  private def reBuildStreamsFromSpecs() = {
    this.defaultOutStreamSpec.map { streamSpec =>
      this.defaultOutStream = Some(streamSpec.buildWritableStream)
    }

    this.externalOstreamsSpec.foreach {streamSpec => {
      this.externalOstreams += streamSpec.buildWritableStream
    }}

    this.istreamsSpec.foreach { streamSpec => {
      this.istreams += streamSpec.buildReadableStream
    }}
  }

  private def addSinkedSPN(spn: SPN): Unit = {
    this.sinkedSPNs += spn
  }

  private def setLinkedSPN(spn: SPN): Unit = {
    this.linkedSpn = Some(spn)
  }

  /**
   * This is called by the execution engine on the cluster node when the topology begins executing
   * Tasks here are
   *   --initialize and subscribe to all input streams
   *   --initialize output streams
   * The partition
   */
  private[etherd] def initialize(partition: Int = 0): Unit = {
    logDebug(s"SPNID:$spnId for topology $topologyName: Begin process streams for partition: $partition")

    // Build all the streams from their specs
    this.reBuildStreamsFromSpecs()

    //init default output stream
    this.defaultOutStream.map { stream => {
      logDebug(s"SPNID:$spnId for topology $topologyName: init default outstream:" + stream.topic)
      stream.init(partition)
    }
    }

    // init all external output streams
    this.externalOstreams.foreach {

      stream => stream.init(partition)
    }

    // init all input streams. SPN should be ready to process events once init completes
    this.istreams.foreach { stream => {
      logDebug(s"SPNID:$spnId for topology $topologyName: Calling init for input stream:" + stream.topic.toString)
      stream.subscribe((topic: String, event: Event) => {
        processEvent(topic, event)
        true
      })
      stream.init(partition)
    }
    }

    this.linkedSpn.map { spn => spn.initialize(partition)}

    //this.currentState = SPNState.Running
  }
}

object SPN {
  val spnIdInc = new AtomicInteger(1)

  def newId(): Int = {
    this.spnIdInc.getAndIncrement
  }
}
