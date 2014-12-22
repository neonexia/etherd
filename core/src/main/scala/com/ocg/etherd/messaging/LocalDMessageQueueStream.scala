package com.ocg.etherd.messaging

import java.util.concurrent.{Executors, ExecutorService}
import com.ocg.etherd.streams.{WriteableEventStream, ReadableEventStream, Event}
import scala.collection.immutable

/**
 *
 */
class LocalDMessageQueue() extends DMessageQueue {
  var qmap = immutable.HashMap[String, immutable.HashMap[Int, DMessageQueueStream]] ()

  def getEventQueue(topic: String, partition: Int): DMessageQueueStream = synchronized {
    qmap.get(topic) match {
      case Some(value) => {
        value.get(partition) match {
          case Some(part) => part
          case None => {
            val stream = new LocalDMessageQueueStream(topic)
            var pl = qmap.get(topic).get
            pl += partition -> stream
            stream
          }
        }
      }
      case None => {
        val stream = new LocalDMessageQueueStream(topic)
        var pl = immutable.HashMap[Int, DMessageQueueStream]()
        pl += 0 -> stream
        qmap += topic -> pl
        stream
      }
    }
  }

  def getEventQueue(topic: String): DMessageQueueStream = {
    getEventQueue(topic, 0)
  }

  def buildFrom(ostream: DMessageQueueStream) : DMessageQueueStream  = {
    getEventQueue(ostream.topic)
  }
}

class LocalDMessageQueueStream(name: String) extends DMessageQueueStream {

  var queue = immutable.List[Event]()
  val pool: ExecutorService = Executors.newFixedThreadPool(2)

  def topic = this.name

  override def push(event: Event): Unit = {
    synchronized {
      queue = queue :+ event
    }
    pool.execute(new Runnable {
      override def run(): Unit = {
        publish(topic, event)
      }
    })
  }

  override def push(events: Iterator[Event]): Unit = {
    events.foreach { event => this.push(event)}
  }

  override def take(offset: Int, count: Int): Iterator[Event] = {
    this.queue.takeRight(count + 1 - offset).iterator
  }
}
