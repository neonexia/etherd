package com.ocg.etherd.streams

import java.nio.ByteBuffer
import scala.reflect.ClassTag

class Tuple(record: Array[Byte]) {

  def getRaw: Array[Byte] = this.record

  def getRecord[T: ClassTag]:T = {
    ???
  }

  def getRecordAsString: String = {
    new String(this.getRaw)
  }

  def getRecordAsInt:Int = {
    ByteBuffer.wrap(this.getRaw).getInt
  }
}

object Tuple {
  def apply(record: Int): Tuple = {
    new Tuple(ByteBuffer.allocate(4).putInt(record).array)
  }

  def apply(record: Array[Byte]): Tuple = {
    new Tuple(record)
  }
}

/**
 * Event class models an immutable business event that should be processed as unit.
 * -order defines global ordering to the event within the topic.
 * -key is an opaque structure as far as the system is concerned and can be used for partitioning,
 * routing and filtering.
 * -record is a of type Tuple which wraps event data
 */
class Event(key: Array[Byte], record: Tuple, order: Long = 0) {
  def getKey = this.key
  def getRecord = this.record
  def getOrder = this.order
}

object Event {

  def apply(key: Int, record: Int, order: Long):Event = {
    val bbKey = ByteBuffer.allocate(4)
    val bbVal = ByteBuffer.allocate(4)
    new Event(bbKey.putInt(key).array, Tuple(record), order)
  }

  def apply(key: String, record: Int, order: Long = 0):Event = {
    new Event(key.getBytes, Tuple(record), order)
  }

  def apply(key: Array[Byte], record: Array[Byte], order: Long): Event = {
    new Event(key, Tuple(record), order)
  }

  def clone(ev: Event): Event  = {
    Event(ev.getKey, ev.getRecord.getRaw, ev.getOrder)
  }

  def keyAsString(event: Event) : String = {
    new String(event.getKey)
  }

  def keyAsInt(event: Event) : Int = {
    ByteBuffer.wrap(event.getKey).getInt
  }
}
