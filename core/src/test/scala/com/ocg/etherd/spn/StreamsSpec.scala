package com.ocg.etherd.spn

import com.ocg.etherd.testbase.UnitSpec
import com.ocg.etherd.streams._
import scala.collection.mutable

class StreamsSpec extends UnitSpec {

  "A pass through spn" should "pass all events from an input stream to output stream" in {
      val q = mutable.Queue[Event]()
      val istream = EventStream.sampleRange("default", 10)
      val wstream = EventStream.sampleWritabletream(q)
      val spn = SPN.pass(this.simple(istream, wstream))
      spn.beginProcessStreams()
      istream.run()
      assertResult(10) {q.size}
    }
  it should "pass all events from all 2 streams to output stream" in {
    val q = mutable.Queue[Event]()
    val istream1 = EventStream.sampleRange("default", 10)
    val istream2 = EventStream.sampleRange("default", 10)
    val wstream = EventStream.sampleWritabletream(q)
    val spn = SPN.pass(this.simple(Set(istream1, istream2), wstream))
    spn.beginProcessStreams()
    istream1.run()
    istream2.run()
    assertResult(20) {
      q.size
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
    val wstream = EventStream.sampleWritabletream(outq)
    val spn = SPN.filter(this.simple(istream, wstream), List("#baddata"))
    spn.beginProcessStreams()
    istream.run()
    assertResult(2) {
      outq.size
    }
  }

  it should "be able to handle empty input streams" in {
    val inq = mutable.Queue[Event]()
    val istream = EventStream.sampleRange("default", inq.iterator)

    val outq = mutable.Queue[Event]()
    val wstream = EventStream.sampleWritabletream(outq)
    val spn = SPN.filter(this.simple(istream, wstream), List("#baddata"))
    spn.beginProcessStreams()
    istream.run()
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
    val wstream = EventStream.sampleWritabletream(outq)
    val spn = SPN.filter(this.simple(istream, wstream), List("#baddata"))
    spn.beginProcessStreams()
    istream.run()
    assertResult(0) {outq.size}
  }
}
