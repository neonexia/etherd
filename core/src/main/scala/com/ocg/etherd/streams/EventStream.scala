package com.ocg.etherd.streams

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable

trait EventStream extends Runnable{
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

trait WriteableEventStream extends EventStream {

  def topic: String

  def push(event: Event)

  def push(events: Iterator[Event])

  def run() {}
}

class NullWriteableEventStream(streamTopic: String) extends  WriteableEventStream {

  def topic = this.streamTopic

  def push(event: Event) {}

  def push(events: Iterator[Event]) {}
}

object EventStream {

  def sampleRange(topic: String, range: Int): ReadableEventStream = {
    val events = (0 until range).zipWithIndex.map(t => Event(t._1, t._2))
    this.sampleRange(topic, events.iterator)
  }

  def sampleRange(topic:String, iter: Iterator[Event], publishDelay: Int=0) = {
    new ReadableEventStream  {
      def topic = "default"
      def take(offset: Int, count: Int): Iterator[Event] = {
        iter.take(count)
      }

      def run(): Unit = {
        iter.foreach { event =>
          this.publish(topic, event)
          if (publishDelay > 0) {
            Thread.sleep(publishDelay)
          }
        }
      }
    }
  }

  def emptyRange(): ReadableEventStream = {
    def topic = "default"
    sampleRange(topic, 0)
  }

  def sampleWritabletream(q: mutable.Queue[Event]): WriteableEventStream = {
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
}

