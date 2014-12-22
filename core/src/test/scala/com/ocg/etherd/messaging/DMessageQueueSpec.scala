package com.ocg.etherd.messaging

import com.ocg.etherd.testbase.UnitSpec
import com.ocg.etherd.streams._
import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.mutable

/**
  */
class DMessageQueueSpec extends UnitSpec {
  "A LocalDMessageQueue" should "return a DMessageQueueStream that can be published and subscribed to" in {
    // Init streams and out queues
    val mq = new LocalDMessageQueue()
    val mstream = mq.getEventQueue("default")
    val outq = new ConcurrentLinkedQueue[Event]()
    val wstream = EventStream.sampleWritablestream(outq)

    // start the topology
    val spn = this.buildPass
    spn.attachInputStream(mstream)
    spn.defaultOutStream = wstream
    spn.beginProcessStreams()

    // simulate the producer
    simulateProducer(mstream, 10)

    // wait for the events to pass through the topology
    // into our awesome concurrent queue
    Thread.sleep(2000)
    assertResult(10) { outq.size }
  }

  it should "be able to build a new DMessageStream that can subscribe to events from another DMessageStream " in {
    val mq = new LocalDMessageQueue()
    val ostream = mq.getEventQueue("default")
    val istream = mq.buildFrom(ostream)
    val outq = new ConcurrentLinkedQueue[Event]()
    istream.subscribe((topic, event) => {
      outq.add(event)
      true
    })
    simulateProducer(ostream, 10)
    Thread.sleep(2000)
    assertResult(10) { outq.size }
  }

  it should "be able to build new DMessageStream that can be subscribed to by multiple target SPNs" in {
    val finalDestinationq = new ConcurrentLinkedQueue[Event]()
    val wstream = EventStream.sampleWritablestream(finalDestinationq)
    val ingestStream = EventStream.sampleRange("ingest", 10)

    val mq = new LocalDMessageQueue()
    val ostream = mq.getEventQueue("default")
    val spn = ingest(ostream, ingestStream)
    spn.beginProcessStreams()

    val target1 = buildPass
    target1.attachInputStream(mq.buildFrom(ostream))
    target1.defaultOutStream = wstream
    target1.beginProcessStreams()

    val target2 = buildPass
    target2.attachInputStream(mq.buildFrom(ostream))
    target2.defaultOutStream = wstream
    target2.beginProcessStreams()

    ingestStream.run()

    Thread.sleep(2000)
    assertResult(20) { finalDestinationq.size }
  }

  it should "allow subscription on multiple partitioned streams for a given topic" in {
    // create an out stream
    val finalDestinationq = new ConcurrentLinkedQueue[Event]()
    val wstream = EventStream.sampleWritablestream(finalDestinationq)

    // create partitioned streams on DMessageQueue and
    // spns for each partitioned stream
    val mq = new LocalDMessageQueue()
    val istreampart1 = mq.getEventQueue("defaultIn")
    val istreampart2 = mq.getEventQueue("defaultIn", 1)
    val istreampart3 = mq.getEventQueue("defaultIn", 2)
    val istreampart4 = mq.getEventQueue("defaultIn", 3)

    val spn1 = ingest(wstream, istreampart1, istreampart3)
    val spn2 = ingest(wstream, istreampart2, istreampart4)
    spn1.beginProcessStreams()
    spn2.beginProcessStreams()

    // spns are ready to process the streams
    // ingest some events into all the partitioned streams
    simulateProducer(istreampart1, 10)
    simulateProducer(istreampart2, 10)
    simulateProducer(istreampart3, 10)
    simulateProducer(istreampart4, 10)

    Thread.sleep(2000)
    assertResult(40) { finalDestinationq.size }
  }
}
