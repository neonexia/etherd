package com.ocg.etherd.messaging

import java.util.concurrent.{Executors, ExecutorService}
import com.ocg.etherd.streams.{WritableEventStream, ReadableEventStream, Event}
import scala.collection.{mutable, immutable}
import com.ocg.etherd.streams._

/**
 * A DMessageBus implementation used for single process (JVM) runtime
 * This is mostly used for testing and single machine non-production scenarios
 */
private[etherd] class LocalDMessageBus() extends DMessageBus {
  var qmap = mutable.HashMap.empty[String, mutable.HashMap[Int, LocalDMessageQueue]]
  //println("Creating a LocalDMessageBus")
  def buildStream(topic: String) : ReadableEventStream  = {
    new LocalReadableDMessageBusStream(topic, this)
  }

  def buildWriteOnlyStream(topic: String) : WritableEventStream  = {
    new LocalWritableDMessageBusStream(topic, this)
  }

  private[etherd] def getLocalQueue(topic: String, partition: Int): LocalDMessageQueue = synchronized {
    qmap.get(topic) match {
      case Some(value) => {
        value.get(partition) match {
          case Some(queue) => queue
          case None => {
            val queue = new LocalDMessageQueue(topic, partition)
            value += partition -> queue
            queue
          }
        }
      }
      case None => {
        var pl = mutable.HashMap.empty[Int, LocalDMessageQueue]
        qmap += topic -> pl
        val queue = new LocalDMessageQueue(topic, partition)
        pl += partition -> queue
        queue
      }
    }
  }
}

private[etherd] class LocalDMessageQueue(name: String, partition: Int) {
  var queue = mutable.ListBuffer[Event]()
  var subscriptions = mutable.ListBuffer[LocalReadableDMessageBusStream]()

  def enqueue(ev: Event): Unit = {
    //println(s"LocalDMessageQueue: Queueing for queue $name on partition $partition")
    queue += ev
    subscriptions.foreach { stream => {
        //println(s"LocalDMessageQueue: Notifying for queue $name on partition $partition")
        stream.notify(ev)
      }
    }
  }

  def subscribe(stream: LocalReadableDMessageBusStream): Unit = {
    //println(s"LocalDMessageQueue: Received subscription for queue $name on partition $partition")
    subscriptions += stream
  }

  private[etherd] def size = this.queue.size
}

private[etherd] class LocalReadableDMessageBusStream(name: String, bus: LocalDMessageBus) extends ReadableEventStream {
  var pool: ExecutorService = Executors.newFixedThreadPool(2)
  var queue: Option[LocalDMessageQueue] = None
  var initializedWithPartition: Int = -1

  override def topic: String = this.name

  def writeOnly = false

  def init(partition: Int): Unit = synchronized {
    if(initializedWithPartition > -1) {
      throw new Exception(s"LocalReadableDMessageBusStream has already been initialized with $initializedWithPartition")
    }

    this.initializedWithPartition = partition
    //println("init LocalReadableDMessageBusStream " + this.name + " for partition" + partition.toString)
    this.queue = Some(this.bus.getLocalQueue(this.topic, partition))
    this.queue.get.subscribe(this)
  }

  def notify(event: Event): Unit = {
    //println("LocalReadableDMessageBusStream: Received event on stream" + this.name)
    pool.execute(new Runnable {
      override def run(): Unit = {
        publish(topic, event)
      }
    })
  }

  override def take(offset: Int, count: Int): Iterator[Event] = {
    null
  }

  private[etherd] def getBackingQueue = this.queue
}

private[etherd] class LocalWritableDMessageBusStream(name: String, bus: LocalDMessageBus) extends WritableEventStream {
  var pool: ExecutorService = Executors.newFixedThreadPool(2)
  var queue: Option[LocalDMessageQueue] = None
  var initialized = false

  def topic = this.name

  def writeOnly = true

  def init(partition: Int): Unit = {
    if(initialized) {
      return
    }

    this.initialized = true
    //println("init LocalWritableDMessageBusStream " + this.name + " for partition" + partition.toString)
    this.queue = Some(this.bus.getLocalQueue(this.topic, partition))
  }

  private[etherd] def getBackingQueue = this.queue

  override def push(event: Event): Unit = {
    //println("LocalWritableDMessageBusStream: Sending event on stream " + this.name)
    this.queue.get.enqueue(event)
  }

  override def push(events: Iterator[Event]): Unit = {
    events.foreach { event => this.push(event)}
  }
}

private[etherd] class LocalDMessageBusStreamBuilder(name: String) extends EventStreamBuilder {
  val messageBus = new LocalDMessageBus()

  def buildReadableStream(spec: ReadableEventStreamSpec): ReadableEventStream = {
    messageBus.buildStream(spec.topic)
  }

  def buildWritableStream(spec: WritableEventStreamSpec): WritableEventStream = {
    messageBus.buildWriteOnlyStream(spec.topic)
  }
}