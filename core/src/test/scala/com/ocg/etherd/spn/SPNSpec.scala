package com.ocg.etherd.spn

import java.util.concurrent.ConcurrentLinkedQueue

import com.ocg.etherd.messaging.{LocalReadableDMessageBusStream, LocalDMessageBus}
import com.ocg.etherd.testbase.UnitSpec
import com.ocg.etherd.streams._
import com.ocg.etherd.topology.{EtherdEnv, Stage}
import scala.collection.mutable

/**
 */
class SPNSpec extends UnitSpec {
  "A pass through spn" should "pass all events from an input stream to output stream" in {
    val q = mutable.Queue[Event]()
    val istream = EventStream.sampleRange("default", 10)
    val wstream = EventStream.sampleWritablestream(q)
    val spn = ingest(wstream, istream)
    spn.beginProcessStreams()

    assertResult(10) {
      q.size
    }
  }

  it should "pass all events from all 2 streams to output stream" in {
    val q = mutable.Queue[Event]()
    val istream1 = EventStream.sampleRange("default", 10)
    val istream2 = EventStream.sampleRange("default", 10)
    val wstream = EventStream.sampleWritablestream(q)
    val spn = ingest(wstream, istream1, istream2)
    spn.beginProcessStreams()
    assertResult(20) {
      q.size
    }
  }

  it should "when combined with 2 sinked Passthrough  SPN's should fan out all events to both the SPN's" in {
    // final desitination sink
    val q = new ConcurrentLinkedQueue[Event]()
    val wstream = EventStream.sampleWritablestream(q)

    // create some streams on the bus. This is where we will ingest data into
    val bus = new LocalDMessageBus()
    val default_mstream = bus.buildStream("default")

    // create a simple toplogy with one pass spn sinking to 2 pass spns.
    // first spn subscribes to the bus stream
    val spn = pass
    spn.attachInputStream(default_mstream)
    val sink1spn = pass
    sink1spn.setdefaultOutputStream(wstream)
    val sink2spn = pass
    sink2spn.setdefaultOutputStream(wstream)
    spn.sink(List(sink1spn, sink2spn))

    // start the topology. The topology will subscribe to the bus for events and process them
    spn.beginProcessStreams(0)
    sink1spn.beginProcessStreams(0)
    sink2spn.beginProcessStreams(0)

    // ingest some data into the bus
    val default_wstream = bus.buildWriteOnlyStream("default")
    default_wstream.init(0)
    simulateProducer(default_wstream, 20)

    Thread.sleep(1000)

    assertResult(40) {
      q.size
    }
  }

  it should "build a single stage" in {
    val spn = ingest()
    var finalStageList = mutable.ListBuffer.empty[Stage]
    spn.buildStages(finalStageList)
    assertResult(1) {
      finalStageList.size
    }
  }

  it should "if combined with MappedSPN build a single stage" in {
    val spn = ingest()
    spn.map {ev => ev}
    var finalStageList = mutable.ListBuffer.empty[Stage]
    spn.buildStages(finalStageList)
    assertResult(1) {
      finalStageList.size
    }
  }

  it should "when sink into 2 SPN's build 3 stages" in {
    val spn = ingest()
    val spn1 = ingest()
    val spn2 = ingest()
    spn.sink(List[SPN](spn1, spn2))
    var finalStageList = mutable.ListBuffer.empty[Stage]
    spn.buildStages(finalStageList)
    assertResult(3) {
      finalStageList.size
    }
  }


  "A FilterSPN" should "filter events with keys #baddata to the output stream" in {
    val inq = mutable.Queue[Event]()
    inq.enqueue(Event("gooddata", 1))
    inq.enqueue(Event("#baddata", 2))
    inq.enqueue(Event("gooddata", 5))
    inq.enqueue(Event("#baddata", 4))
    inq.enqueue(Event("#baddata", 6))
    val istream = EventStream.sampleRange("default", inq.iterator)

    val outq = mutable.Queue[Event]()
    val wstream = EventStream.sampleWritablestream(outq)

    val spn = ingest(wstream, istream)
    val filterSpn = spn.filterByKeys(List("#baddata"))

    spn.beginProcessStreams()

    assertResult(2) {
      outq.size
    }
  }

  it should "be able to handle empty input streams" in {
    val inq = mutable.Queue[Event]()
    val istream = EventStream.sampleRange("default", inq.iterator)

    val outq = mutable.Queue[Event]()
    val wstream = EventStream.sampleWritablestream(outq)
    val spn = ingest(wstream, istream)
    spn.filterByKeys(List("#baddata"))
    spn.beginProcessStreams()
    assertResult(0) {outq.size}
  }

  it should "be able to handle empty output streams" in {
    val inq = mutable.Queue[Event]()
    inq.enqueue(Event("#baddata", 1))
    inq.enqueue(Event("#baddata", 2))
    inq.enqueue(Event("#baddata", 5))
    inq.enqueue(Event("#baddata", 4))
    val istream = EventStream.sampleRange("default", inq.iterator)

    val outq = mutable.Queue[Event]()
    val wstream = EventStream.sampleWritablestream(outq)
    val spn = ingest(wstream, istream)
    val filterSpn = spn.filterByKeys(List("#baddata"))
    spn.beginProcessStreams()
    assertResult(0) {outq.size}
  }

  it should "when combined with 1 linked and 1 sinked Filter SPN's and should apply both filters" in {
    // final desitination sink
    val q = new ConcurrentLinkedQueue[Event]()
    val wstream = EventStream.sampleWritablestream(q)

    val q2 = new ConcurrentLinkedQueue[Event]()
    val w2stream = EventStream.sampleWritablestream(q2)

    // create some streams on the bus. This is where we will ingest data into
    val bus = new LocalDMessageBus()
    val default_wstream = bus.buildWriteOnlyStream("default") // --> entry point stream where we will ingest data into
    val default_mstream = bus.buildStream("default") // --> same as above but we willr ead data from
    default_wstream.init(0)

    ///// build the topology

    val env = new EtherdEnv("topology")  // --> env

    // create 2 sink spns
    val f2spn = new FilterKeysSPN(env, List("#secondFilter"))
    f2spn.setdefaultOutputStream(wstream)
    val p2spn = new PassThroughSPN(env)
    p2spn.setdefaultOutputStream(w2stream)

    val ingestSpn = new PassThroughSPN(env) // --> ingestion SPN
    ingestSpn.attachInputStream(default_mstream)
    ingestSpn.filterByKeys(List("#firstFilter")).sink(List(f2spn, p2spn)) // --->> define the topology

    // --> start the stream processing on all spns
    f2spn.beginProcessStreams(0)
    p2spn.beginProcessStreams(0)
    ingestSpn.beginProcessStreams(0)

    // now push some events through the
    new Thread {
      override def run(): Unit ={

        for (i <- 0 until 20) {
          default_wstream.push(Event("#cleandata", i))
        }
        default_wstream.push(Event("#firstFilter", 20 ))
        default_wstream.push(Event("#secondFilter", 21))
      }
    }.start()

    Thread.sleep(1000)
    assertResult(20) {
      q.size
    }
  }
}
