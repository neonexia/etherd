package com.ocg.etherd.runtime

import java.util.concurrent.ConcurrentLinkedQueue

import com.ocg.etherd.EtherdEnv
import com.ocg.etherd.messaging.LocalDMessageBus
import com.ocg.etherd.messaging.LocalReadableDMessageBusStream
import com.ocg.etherd.messaging.{LocalReadableDMessageBusStream, LocalDMessageBus}
import com.ocg.etherd.testbase.UnitSpec
import com.ocg.etherd.streams._
import com.ocg.etherd.testbase.UnitSpec
import com.ocg.etherd.topology.Stage
import scala.collection.mutable

/**
 * Tests for execution of stages using local message bus and local thread scheduler
  */
class LocalExecutionSpec extends UnitSpec {
  "A topology with single stage" should "execute send data to destination output stream" in {
    // create a target destination stream for the topology
    // final desitination sink
    val q = new ConcurrentLinkedQueue[Event]()
    val wstream = EventStream.sampleWritablestream(q)


  }
}
