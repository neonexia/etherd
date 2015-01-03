package com.ocg.etherd.messaging

import java.util.concurrent.{Executors, ExecutorService}
import com.ocg.etherd.streams.{WriteableEventStream, ReadableEventStream, Event}
import scala.collection.{mutable, immutable}

/**
 *
 */
private[etherd] class LocalDMessageBus() extends DMessageBus {
  var qmap = mutable.HashMap.empty[String, mutable.HashMap[Int, LocalDMessageQueue]]

  def buildStream(topic: String) : ReadableEventStream  = {
    new LocalReadableDMessageBusStream(topic, this)
  }

  def buildWriteOnlyStream(topic: String) : WriteableEventStream  = {
    new LocalWritableDMessageBusStream(topic, this)
  }

  private[etherd] def getLocalQueue(topic: String, partition: Int): LocalDMessageQueue = synchronized {
    qmap.get(topic) match {
      case Some(value) => {
        value.get(partition) match {
          case Some(queue) => queue
          case None => {
            val queue = new LocalDMessageQueue()
            value += partition -> queue
            queue
          }
        }
      }
      case None => {
        var pl = mutable.HashMap.empty[Int, LocalDMessageQueue]
        qmap += topic -> pl

        val queue = new LocalDMessageQueue()
        pl += partition -> queue
        queue
      }
    }
  }
}

private[etherd] class LocalDMessageQueue() {
  var queue = mutable.ListBuffer[Event]()
  var subscriptions = mutable.ListBuffer[LocalReadableDMessageBusStream]()

  def enqueue(ev: Event): Unit = {
    queue += ev
    subscriptions.foreach { stream => stream.notify(ev) }
  }

  def subscribe(stream: LocalReadableDMessageBusStream): Unit = {
    subscriptions += stream
  }

  private[etherd] def size = this.queue.size
}

private[etherd] class LocalReadableDMessageBusStream(name: String, bus: LocalDMessageBus) extends ReadableEventStream {
  var pool: ExecutorService = Executors.newFixedThreadPool(2)
  var queue: Option[LocalDMessageQueue] = None
  var initialized = false
  def topic = this.name

  def writeOnly = false

  def init(partition: Int): Unit = synchronized {
    if(initialized) {
      return
    }

    this.initialized = true
    // println("init LocalReadableDMessageBusStream " + this.name + " for partition" + partition.toString)
    this.queue = Some(this.bus.getLocalQueue(this.topic, partition))
    this.queue.get.subscribe(this)
  }

  def notify(event: Event): Unit = {
    pool.execute(new Runnable {
      override def run(): Unit = {
        publish(topic, event)
      }
    })
  }

  private[etherd] def getBackingQueue = this.queue

  override def take(offset: Int, count: Int): Iterator[Event] = {
    null
  }
}

private[etherd] class LocalWritableDMessageBusStream(name: String, bus: LocalDMessageBus) extends WriteableEventStream {
  var pool: ExecutorService = Executors.newFixedThreadPool(2)
  var queue: Option[LocalDMessageQueue] = None

  def topic = this.name

  def writeOnly = true

  def init(partition: Int): Unit = {
    this.queue = Some(this.bus.getLocalQueue(this.topic, partition))
  }

  private[etherd] def getBackingQueue = this.queue

  override def push(event: Event): Unit = {
    this.queue.get.enqueue(event)
  }

  override def push(events: Iterator[Event]): Unit = {
    events.foreach { event => this.queue.get.enqueue(event)}
  }
}
