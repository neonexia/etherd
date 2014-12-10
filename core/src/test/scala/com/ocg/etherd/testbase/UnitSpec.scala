package com.ocg.etherd.testbase

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
    val ostream = EventStream.sampleWritabletream(q)
    val spn = new PassThroughSPN(new StageExecutionContext())
    spn.attachInputStream(istream)
    spn.defaultOutStream = ostream
    spn
  }

  def ingest(istream: ReadableEventStream, ostream: WriteableEventStream): SPN = {
    ingest(mutable.ListBuffer[ReadableEventStream](istream), ostream)
  }
  def ingest(streams: mutable.ListBuffer[ReadableEventStream], ostream: WriteableEventStream): SPN = {
    val spn = new PassThroughSPN(new StageExecutionContext())
    // println("streams size: " + streams.size )
    streams.foreach{ istream => {
      //println("attaching input stream in test")
      spn.attachInputStream(istream)
    }}
    spn.defaultOutStream = ostream
    spn
  }
}
