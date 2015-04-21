package com.ocg.etherd.runtime

import java.util.concurrent.ConcurrentLinkedQueue

import akka.actor.{Actor, ActorRef, ActorSystem, ActorSelection}
import akka.pattern.ask
import scala.collection.mutable.ListBuffer
import scala.concurrent.Await
import scala.concurrent.duration._
import com.ocg.etherd.EtherdEnv
import com.ocg.etherd.runtime.RuntimeMessages.{ExecutorList, GetRegisteredExecutors}
import com.ocg.etherd.topology.Topology
import com.ocg.etherd.messaging.LocalDMessageBus
import com.ocg.etherd.messaging.LocalReadableDMessageBusStream
import com.ocg.etherd.messaging.{LocalReadableDMessageBusStream, LocalDMessageBus}
import com.ocg.etherd.testbase.UnitSpec
import com.ocg.etherd.streams._
import com.ocg.etherd.testbase.UnitSpec
import com.ocg.etherd.topology.Stage
import scala.collection.mutable
import scala.concurrent.Await

/**
 * Tests for execution of topologies using local message bus and local thread scheduler
 * These tests should also act as an examples on how to build topologies and run them
 * Tests will also exercise graceful start and stop of topologies and fault tolerance
  */
class LocalExecutionSpec extends UnitSpec {
  "A topology" should "with a single keep processing events to the destination output stream until stopped" in {
    // start the cluster manager
    val cmActor = ClusterManager.start()

    // final destination sink
    val destinationStore = buildDummyDestinationStream("final_destination")

    // build the topology and run it.
    val tp = Topology("topology")
    tp.ingest(new ReadableEventStreamSpec("input_stream1")).map(e => Event(e.getKey, e.getRecord.getRaw, e.getOrder))
      .dropByKeys(List("#baddata"))
      .sink(new WritableEventStreamSpec("final_destination"))
    tp.run()

    // wait for executors to start
    Thread.sleep(2000)

    try {
      // veify executor registrations are successful
      val f = cmActor.ask(GetRegisteredExecutors("topology"))(1 seconds).mapTo[ExecutorList]
      val registeredExecutors = Await.result(f, 1 seconds)
      registeredExecutors.executors.size should equal (1)

      // write events into the ingestion stream
      produceEvents("input_stream1", 10)
      Thread.sleep(100)
      //hope events make it to the destination
      destinationStore.size should equal (10)

      // write more events the ingestion stream
      produceEvents("input_stream1", 10)
      Thread.sleep(100)
      //hope again we have the new events make it to the destination
      destinationStore.size should equal (20)
    }
    finally{
      cmShutdown()
      shutdownTasks(EtherdEnv.get)
    }
   }

   it should "with multiple stages keep processing events to the destination output stream until stopped" in {
    // start the cluster manager
    val cmActor = ClusterManager.start()

    // final destination sink
    val destinationStore = buildDummyDestinationStream("final_destination")

    // build the topology and run it.
    val tp = Topology("topology")
    val spn = tp.ingest(new ReadableEventStreamSpec("input_stream1"))
      .map(e => Event(e.getKey, e.getRecord.getRaw, e.getOrder))
      .dropByKeys(List("#baddata"))
      .sink(buildPass)
      .sink(new WritableEventStreamSpec("final_destination"))
    val stageList = ListBuffer.empty[Stage]
    tp.run()

    // wait for executors to start and the topology to process events
    Thread.sleep(1000)

    try {
      // ask the clusterManager if executor registrations are successful
      val f = cmActor.ask(GetRegisteredExecutors("topology"))(1 seconds).mapTo[ExecutorList]
      val registeredExecutors = Await.result(f, 1 seconds)
      registeredExecutors.executors.size should equal (2)

      // write events into the ingestion stream
      produceEvents("input_stream1", 10)
      Thread.sleep(100)
      //hope the events make it to the destination
      destinationStore.size should equal (10)

      // write more events the ingestion stream
      produceEvents("input_stream1", 10)
      Thread.sleep(100)

      //hope again we have the new events make it to the destination
      destinationStore.size should equal (20)
    }
    finally{
      cmShutdown()
      shutdownTasks(EtherdEnv.get)
    }
  }

  "2 topologies" should "each with multiple stages when run in parallel should process events" in {
    // start the cluster manager
    val cmActor = ClusterManager.start()
    // final destination sink
    val destinationStore = buildDummyDestinationStream("final_destination")

    // build the topology and run it.
    val tp = Topology("testtopology1")
    tp.ingest(new ReadableEventStreamSpec("input_stream1"))
      .map(e => Event(e.getKey, e.getRecord.getRaw, e.getOrder))
      .dropByKeys(List("#baddata"))
      .sink(buildPass)
      .sink(new WritableEventStreamSpec("final_destination"))
    tp.run()

    // build another topology and run it.
    val tp2 = Topology("testtopology2")
    tp2.ingest(new ReadableEventStreamSpec("input_stream2"))
      .map(e => Event(e.getKey, e.getRecord.getRaw, e.getOrder))
      .sink(buildPass)
      .sink(new WritableEventStreamSpec("final_destination"))
    tp2.run()

    //wait for executors to start
    Thread.sleep(1000)

    try {
      // write events into the streams
      produceEvents("input_stream1", 15)
      produceEvents("input_stream2", 12)
      Thread.sleep(200)

      destinationStore.size should equal (27)
    }
    finally{
      cmShutdown()
      shutdownTasks(EtherdEnv.get)
    }
  }
}
