package com.ocg.etherd.spn

import com.ocg.etherd.testbase.UnitSpec
import com.ocg.etherd.streams._
import com.ocg.etherd.topology.{StageContext, TopologyContext}
import scala.collection.mutable

class StreamsSpec extends UnitSpec {

  "A pass through spn" should "pass all events from an input stream to output stream" in {
      val q = mutable.Queue[Event]()
      val istream = EventStream.range("default", 10)
      val wstream = EventStream.writableQueue(q)
      val spn = SPN.pass(this.simple(istream, wstream))
      spn.beginProcessStreams()
      istream.start()
      assert(q.size == 10)
    }
  it should "pass all events from all 2 streams to output stream" in {
    val q = mutable.Queue[Event]()
    val istream1 = EventStream.range("default", 10)
    val istream2 = EventStream.range("default", 10)
    val wstream = EventStream.writableQueue(q)
    val spn = SPN.pass(this.simple(Set(istream1, istream2), wstream))
    spn.beginProcessStreams()
    istream1.start()
    istream2.start()
    assert(q.size == 20)
  }
}
