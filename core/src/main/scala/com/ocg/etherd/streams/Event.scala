package com.ocg.etherd.streams

import java.nio.ByteBuffer

/**
 */
class Event(key: Array[Byte], record: Array[Byte], order: Long = 0) {
  def getKey = this.key
  def getRecord = this.record
  def getOrder = this.order
}

object Event {

  def apply(key: Int, record: Int, order: Long):Event = {
    val bbKey = ByteBuffer.allocate(4)
    val bbVal = ByteBuffer.allocate(4)
    new Event(bbKey.putInt(key).array, bbVal.putInt(record).array, order)
  }

  def apply(key: String, record: Int, order: Long = 0):Event = {
    val bbVal = ByteBuffer.allocate(4)
    new Event(key.getBytes, bbVal.putInt(record).array, order)
  }

  def apply(key: Array[Byte], record: Array[Byte], order: Long): Event = {
    new Event(key, record, order)
  }

  def keyAsString(event: Event) : String = {
    new String(event.getKey)
  }

  def keyAsInt(event: Event) : Int = {
    ByteBuffer.wrap(event.getKey).getInt
  }
}
