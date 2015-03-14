package com.ocg.etherd.streams

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.mutable

trait EventStream {
  def topic: String
  def windowSize = 0
  def init(partition: Int): Unit
}

trait EventSubscription {
  var nextId: AtomicInteger = new AtomicInteger(0)
  val subs = mutable.Map[Int, (String, Event) => Boolean]()

  def subscribe(f: (String, Event) => Boolean): Int = {
    val newId = this.nextId.getAndIncrement
    this.subs.update(newId, f)
    newId
  }
}

trait ReadableEventStream extends EventStream with EventSubscription {

  def topic: String

  def take(offset: Int, count: Int): Iterator[Event]

  def publish(topic: String, event: Event) = {
    this.subs.values.foreach { sub =>
      sub(topic, event)
    }
  }
}

trait WritableEventStream extends EventStream {

  def topic: String

  def push(event: Event)

  def push(events: Iterator[Event])
}

object EventStream {

  def sampleWritablestream(q: mutable.Queue[Event]): WritableEventStream = {
    new WritableEventStream {

      def topic = "default"

      def push(event: Event): Unit = {
        q.enqueue(event)
      }

      def push(events: Iterator[Event]) {
        events.foreach(event => this.push(event))
      }

      def init(partition: Int): Unit = {

      }
    }
  }

  def sampleWritablestream(q: ConcurrentLinkedQueue[Event]): WritableEventStream = {
      new WritableEventStream {
        def topic = "sampleWritableStream"
        def push(event: Event): Unit = {
          q.add(event)
        }

        def push(events: Iterator[Event]) {
          events.foreach(event => this.push(event))
        }

        def init(partition: Int): Unit = {

        }
      }
  }
}

