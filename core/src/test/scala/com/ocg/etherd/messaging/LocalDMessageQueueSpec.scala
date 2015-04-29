package com.ocg.etherd.messaging

import com.ocg.etherd.testbase.UnitSpec
import com.ocg.etherd.streams._
import java.util.concurrent.ConcurrentLinkedQueue

/**
 * Tests for local message bus implementation
  */
class LocalDMessageQueueSpec extends UnitSpec {

  "A LocalDMessageBus" should "build multiple streams with same backing queue when linked to the same partition of the same topic" in {
    val default_mstream0 = buildLocalReadableStream("default")
    val default_mstream1 = buildLocalReadableStream("default")
    val default_wstream0 = buildLocalWritableStream("default")

    default_mstream0.init(0)
    default_mstream1.init(0)
    default_wstream0.init(0)

    assert(default_mstream0.getBackingQueue.nonEmpty)
    assert(default_mstream1.getBackingQueue.nonEmpty)
    assertResult(true) {
      default_mstream0.getBackingQueue.get == default_mstream1.getBackingQueue.get
    }

    assertResult(true) {
      default_wstream0.getBackingQueue.get == default_mstream0.getBackingQueue.get
    }

    val default1_dmstream0 = buildLocalReadableStream("default1")
    val default1_dmstream1 = buildLocalReadableStream("default1")
    default1_dmstream0.init(0)
    default1_dmstream1.init(0)
    assert(default1_dmstream0.getBackingQueue.nonEmpty)
    assert(default1_dmstream1.getBackingQueue.nonEmpty)

    assertResult(true) {
      default1_dmstream0.getBackingQueue.get eq default1_dmstream1.getBackingQueue.get
    }

    assertResult(true) {
      default1_dmstream0.getBackingQueue.get ne default_mstream0.getBackingQueue.get
    }
  }

  it should "be able to build streams with their own backing queues when linked to different partitions for the same topic" in {
    val bus = new LocalDMessageBus()
    val mstream0 = buildLocalReadableStream("default")
    val mstream1 = buildLocalReadableStream("default")
    val wstream0 = buildLocalWritableStream("default")
    val wstream1 = buildLocalWritableStream("default")

    mstream0.init(0)
    wstream0.init(0)

    mstream1.init(1)
    wstream1.init(1)

    assert(mstream0.getBackingQueue.nonEmpty)
    assert(mstream1.getBackingQueue.nonEmpty)
    assert(wstream0.getBackingQueue.nonEmpty)
    assert(wstream1.getBackingQueue.nonEmpty)

    assertResult(true) {
      mstream0.getBackingQueue.get ne mstream1.getBackingQueue.get
    }

    assertResult(true) {
      wstream0.getBackingQueue.get ne wstream1.getBackingQueue.get
    }

    assertResult(true) {
      mstream1.getBackingQueue.get eq wstream1.getBackingQueue.get
    }
  }

  it should "be able to build multiple streams on the same topic that can be subscribed to" in {
    // final output target
    val outq = new ConcurrentLinkedQueue[Event]()

    // Init streams and out queues
    val bus = new LocalDMessageBus()
    val mstream = buildLocalReadableStream("default")
    val mstream1 = buildLocalReadableStream("default")
    val wstream = buildLocalWritableStream("default")
    mstream.init(0)
    mstream.subscribe((topic, ev) => {
      outq.add(ev)
    })

    mstream1.init(0)
    mstream1.subscribe((topic, ev) => {
      outq.add(ev)
    })

    wstream.init(0)
    this.produceEvents(wstream, 10)

    Thread.sleep(1000)

    assertResult(10) {
      wstream.getBackingQueue.get.size
    }

    assertResult(20) {
      outq.size
    }
  }

  it should "be able to build readable and writable streams on different partitions of the same topic that can be subscribed to" in {
    // final output target
    val outq0 = new ConcurrentLinkedQueue[Event]()
    val outq1 = new ConcurrentLinkedQueue[Event]()

    // Init streams and out queues
    val bus = new LocalDMessageBus()
    val mstream0 = buildLocalReadableStream("default")
    val mstream1 = buildLocalReadableStream("default")
    val wstream0 = buildLocalWritableStream("default")
    val wstream1 = buildLocalWritableStream("default")

    // read from partition 0
    mstream0.init(0)
    mstream0.subscribe((topic, ev) => {
      outq0.add(ev)
    })

    // write to partition 0 only
    wstream0.init(0)
    this.produceEvents(wstream0, 25)

    Thread.sleep(1000)

    assertResult(25) {
      wstream0.getBackingQueue.get.size
    }

    assertResult(25) {
      outq0.size
    }

    // read from partition 1
    mstream1.init(1)
    mstream1.subscribe((topic, ev) => {
      outq1.add(ev)
    })

    // write to topic "default" on partition 1 only
    wstream1.init(1)

    assertResult(true) {
      mstream1.getBackingQueue.get eq wstream1.getBackingQueue.get
    }

    this.produceEvents(wstream1, 12)

    Thread.sleep(1000)

    assertResult(12) {
      wstream1.getBackingQueue.get.size
    }

    assertResult(12) {
      outq1.size
    }
  }
}
