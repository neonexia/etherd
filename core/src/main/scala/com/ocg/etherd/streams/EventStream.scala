package com.ocg.etherd.streams

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable

trait EventStream {
  def topic: String
  def windowSize = 0
}
trait EventSubscription {
  var nextId: AtomicInteger = new AtomicInteger(0)
  val subs = mutable.Map[Int, (String, Event) => Boolean]()

  def subscribe(f: (String, Event) => Boolean): Int = {
    val newId = this.nextId.getAndIncrement
    this.subs.update(newId, f)
    newId
  }

  def publish(topic: String, event: Event) = {
    this.subs.values.foreach { sub =>
      sub(topic, event)
    }
  }
}

trait ReadableEventStream extends EventStream with EventSubscription {

  def topic: String

  def take(offset: Int, count: Int): Iterator[Event]

  def start()
}

trait WriteableEventStream extends EventStream {

  def topic: String

  def push(event: Event)

  def push(events: Iterator[Event])
}

class NullWriteableEventStream(streamTopic: String) extends  WriteableEventStream {

  def topic = this.streamTopic

  def push(event: Event) {}

  def push(events: Iterator[Event]) {}
}

object EventStream {

  def range(topic: String, range: Int): ReadableEventStream = {
    val events = (0 until range).zipWithIndex.map(t => Event(t._1, t._2))
    new ReadableEventStream {
      def topic = "default"
      def take(offset: Int, count: Int): Iterator[Event] = {
        events.take(count).iterator
      }

      def start(): Unit ={
        events.foreach { event =>
          this.publish(topic, event)
        }
      }
    }
  }

  def writableQueue(q: mutable.Queue[Event]): WriteableEventStream = {
    new WriteableEventStream {

      def topic = "default"

      def push(event: Event): Unit = {
        q.enqueue(event)
      }

      def push(events: Iterator[Event]) {
        events.foreach(event => this.push(event))
      }
    }
  }

  def empty(): ReadableEventStream = {
    def topic = "default"
    range(topic, 0)
  }
}

