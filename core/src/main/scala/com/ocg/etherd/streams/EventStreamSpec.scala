package com.ocg.etherd.streams

trait EventStreamSpec extends Serializable {

  def topic: String

  def Props: Map[String, String]

  def writable: Boolean
}

class ReadableEventStreamSpec(topicName: String) extends EventStreamSpec {

  def topic: String = topicName

  def Props: Map[String, String] = Map.empty[String, String]

  def writable:Boolean = false
}

class WritableEventStreamSpec(topicName: String) extends EventStreamSpec {

  def topic: String = topicName

  def Props: Map[String, String] = Map.empty[String, String]

  def writable:Boolean = true
}

trait EventStreamBuilder {
  def buildReadableStream(spec: ReadableEventStreamSpec): ReadableEventStream

  def buildWritableStream(spec: WritableEventStreamSpec): WritableEventStream
}