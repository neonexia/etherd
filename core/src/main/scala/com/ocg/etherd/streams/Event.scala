package com.ocg.etherd.streams

import java.nio.ByteBuffer

/**
 */
class Event(key: Array[Byte], record: Array[Byte]){
  def getKey = this.key
  def getRecord = this.record
}

object Event {

  def apply(key: Int, record: Int):Event = {
    val bbKey = ByteBuffer.allocate(4)
    val bbVal = ByteBuffer.allocate(4)
    new Event(bbKey.putInt(key).array, bbVal.putInt(record).array)
  }

  def keyAsString(event: Event) : String = {
    new String(event.getKey)
  }

  def keyAsInt(event: Event) : Int = {
    ByteBuffer.wrap(event.getKey).getInt
  }
}
