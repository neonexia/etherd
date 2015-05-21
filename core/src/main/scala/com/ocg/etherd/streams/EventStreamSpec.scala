package com.ocg.etherd.streams

import com.ocg.etherd.EtherdEnv

/**
 * Models event stream specification
 */
trait EventStreamSpec extends Serializable {

  def topic: String

  def Props: Map[String, String] = Map.empty[String, String]

  def buildReadableStream: ReadableEventStream

  def buildWritableStream: WritableEventStream
}

/**
 * Generic implementation of EventStreamSpec that builds a readable event stream
 * off the default message bus
 * @param name
 */
private[etherd] class ReadableStreamSpec(name: String) extends EventStreamSpec {
  def topic = name

  def buildReadableStream: ReadableEventStream = {
    EtherdEnv.get.defaultMessageBus.buildStream(this.topic)
  }

  def buildWritableStream: WritableEventStream = ???
}

/**
 * Generic implementation of EventStreamSpec that builds a writable event stream
 * off the default message bus
 * @param name
 */
private[etherd] class WritableStreamSpec(name: String) extends EventStreamSpec {
  def topic = name

  def buildReadableStream: ReadableEventStream = ???

  def buildWritableStream: WritableEventStream = {
    EtherdEnv.get.defaultMessageBus.buildWriteOnlyStream(this.topic)
  }
}