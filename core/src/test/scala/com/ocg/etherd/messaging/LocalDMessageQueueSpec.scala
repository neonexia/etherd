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

  it should "be able to build readable and writable streams on different partitions of the different topics that can be subscribed to" in {
    // final output target
    val outq1 = new ConcurrentLinkedQueue[Event]()  // destination for topic 1
    val outq2 = new ConcurrentLinkedQueue[Event]()  // destination for topic 2

    // Init streams and out queues
    val bus = new LocalDMessageBus()
    val mstream10 = buildLocalReadableStream("topic1") // topic1 partition 0 readable
    val wstream10 = buildLocalWritableStream("topic1") // topic1 partition 0 writable
    val mstream11 = buildLocalReadableStream("topic1") // topic1 partition 1 readable
    val wstream11 = buildLocalWritableStream("topic1") // topic1 partition 1 writable

    val mstream20 = buildLocalReadableStream("topic2")
    val wstream20 = buildLocalWritableStream("topic2")
    val mstream21 = buildLocalReadableStream("topic2")
    val wstream21 = buildLocalWritableStream("topic2")

    // read from topic 1 partition 0
    mstream10.init(0)
    mstream10.subscribe((topic, ev) => {
      outq1.add(ev)
    })
    // write to topic 1 partition 0
    wstream10.init(0)
    this.produceEvents(wstream10, 25)

    Thread.sleep(1000)
    assertResult(25) {
      wstream10.getBackingQueue.get.size
    }
    assertResult(25) {
      outq1.size
    }

    // read from topic 1 partition 1
    mstream11.init(1)
    mstream11.subscribe((topic, ev) => {
      outq1.add(ev)
    })
    // write to topic 1 partition 1
    wstream11.init(1)
    assertResult(true) {
      mstream11.getBackingQueue.get eq wstream11.getBackingQueue.get
    }
    this.produceEvents(wstream11, 12)

    Thread.sleep(1000)
    assertResult(12) {
      wstream11.getBackingQueue.get.size
    }
    assertResult(37) {
      outq1.size
    }


    // read from topic 2 partition 0
    mstream20.init(0)
    mstream20.subscribe((topic, ev) => {
      outq2.add(ev)
    })
    // write to topic 2 partition 0
    wstream20.init(0)
    this.produceEvents(wstream20, 60)

    Thread.sleep(1000)
    assertResult(60) {
      wstream20.getBackingQueue.get.size
    }
    assertResult(60) {
      outq2.size
    }


    // read from topic 2 partition 1
    mstream21.init(1)
    mstream21.subscribe((topic, ev) => {
      outq2.add(ev)
    })
    // write to topic 2 partition 1
    wstream21.init(1)
    assertResult(true) {
      mstream21.getBackingQueue.get eq wstream21.getBackingQueue.get
    }
    this.produceEvents(wstream21, 28)

    Thread.sleep(1000)
    assertResult(28) {
      wstream21.getBackingQueue.get.size
    }
    assertResult(88) {
      outq2.size
    }
  }

}
