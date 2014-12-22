package com.ocg.etherd.testbase

import com.ocg.etherd.messaging.DMessageQueueStream
import org.scalatest._
import com.ocg.etherd.streams._
import com.ocg.etherd.spn.{PassThroughSPN, SPN}
import com.ocg.etherd.topology.StageExecutionContext
import scala.collection.mutable

/**
 *
 */
abstract class UnitSpec extends FlatSpec with Matchers with
OptionValues with Inside with Inspectors
{
  def ingest(): SPN = {
    val q = mutable.Queue[Event]()
    val istream = EventStream.sampleRange("default", 10)
    val ostream = EventStream.sampleWritablestream(q)
    val spn = new PassThroughSPN(new StageExecutionContext())
    spn.attachInputStream(istream)
    spn.defaultOutStream = ostream
    spn
  }

  def ingest(ostream: WriteableEventStream, streams: ReadableEventStream*): SPN = {
    val spn = new PassThroughSPN(new StageExecutionContext())
    // println("streams size: " + streams.size )
    streams.foreach{ istream => {
      //println("attaching input stream in test")
      spn.attachInputStream(istream)
    }}
    spn.defaultOutStream = ostream
    spn
  }

  def simulateProducer(mstream: DMessageQueueStream, numEvents: Int): Unit = {
     val t = new Thread {
       override def run(): Unit ={
         for (i <- 0 until numEvents) {
           mstream.push(Event(i, i))
         }
       }
     }
    t.start()
  }

  def buildPass: SPN = new PassThroughSPN(new StageExecutionContext())
}
