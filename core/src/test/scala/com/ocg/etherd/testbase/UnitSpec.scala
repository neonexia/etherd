package com.ocg.etherd.testbase

import com.ocg.etherd.messaging.{LocalWritableDMessageBusStream, LocalReadableDMessageBusStream, LocalDMessageBus}
import org.scalatest._
import com.ocg.etherd.streams._
import com.ocg.etherd.spn.{PassThroughSPN, SPN}
import com.ocg.etherd.topology.EtherdEnv

/**
 *
 */
abstract class UnitSpec extends FlatSpec with Matchers with
OptionValues with Inside with Inspectors
{
  def pass = new PassThroughSPN(new EtherdEnv("topology"))

  def ingest(): SPN = {
    val bus = new LocalDMessageBus()
    val istream = new LocalReadableDMessageBusStream("topology_in1", bus)
    val ostream = new LocalWritableDMessageBusStream("topology_out", bus)
    val spn = new PassThroughSPN(new EtherdEnv("topology"))
    spn.attachInputStream(istream)
    spn.setdefaultOutputStream(ostream)
    spn
  }

  def ingest(ostream: WriteableEventStream, istreams: ReadableEventStream*): SPN = {
    val spn = new PassThroughSPN(new EtherdEnv("topology"))
    istreams.foreach{ stream => {
      spn.attachInputStream(stream)
    }}
    spn.setdefaultOutputStream(ostream)
    spn
  }

  def ingest(istreams: ReadableEventStream*): SPN = {
    val spn = new PassThroughSPN(new EtherdEnv("topology"))
    istreams.foreach{ stream => {
      spn.attachInputStream(stream)
    }}
    spn
  }

  def simulateProducer(mstream: WriteableEventStream, numEvents: Int): Unit = {
     val t = new Thread {
       override def run(): Unit ={
         for (i <- 0 until numEvents) {
           mstream.push(Event(i, i))
         }
       }
     }
    t.start()
  }

  def buildPass: SPN = new PassThroughSPN(new EtherdEnv("topology"))
}
