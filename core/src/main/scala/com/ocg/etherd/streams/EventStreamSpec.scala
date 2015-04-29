package com.ocg.etherd.streams

trait EventStreamSpec extends Serializable {

  def topic: String

  def Props: Map[String, String] = Map.empty[String, String]

  def buildReadableStream: ReadableEventStream

  def buildWritableStream: WritableEventStream
}