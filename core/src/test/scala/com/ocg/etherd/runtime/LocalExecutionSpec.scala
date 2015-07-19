package com.ocg.etherd.runtime

import java.util.concurrent.ConcurrentLinkedQueue

import akka.pattern.ask
import scala.collection.mutable.ListBuffer
import scala.concurrent.Await
import scala.concurrent.duration._
import com.ocg.etherd.EtherdEnv
import com.ocg.etherd.runtime.RuntimeMessages.{ExecutorList, GetRegisteredExecutors}
import com.ocg.etherd.topology.Topology
import com.ocg.etherd.streams._
import com.ocg.etherd.testbase.UnitSpec
import com.ocg.etherd.topology.Stage
import scala.concurrent.Await

/**
 * Tests for execution of topologies using local message bus and local thread scheduler
 * These tests should also act as an examples on how to build topologies and run them
 * Tests will also exercise graceful start and stop of topologies and fault tolerance
 */
class LocalExecutionSpec extends UnitSpec {
//  "A topology with a single stage" should "keep processing events to the destination output stream until stopped" in {
//    // start the cluster manager
//    val cmActor = ClusterManager.start()
//
//    // final destination sink
//    val destinationStore = buildDummyDestinationStream("final_destination")
//
//    // build the topology and run it.
//    val tp = Topology("topology")
//    tp.ingest("input_stream1")
//      .map(e => Event(e.getKey, e.getRecord.getRaw, e.getOrder))
//      .dropByKeys(List("#baddata"))
//      .sink(new WritableStreamSpec("final_destination"))
//    tp.run()
//
//    // wait for executors to start
//    Thread.sleep(2000)
//
//    try {
//      // verify executor registrations are successful
//      val f = cmActor.ask(GetRegisteredExecutors("topology"))(1 seconds).mapTo[ExecutorList]
//      val registeredExecutors = Await.result(f, 1 seconds)
//      registeredExecutors.executors.size should equal (1)
//
//      // write events into the ingestion stream
//      produceEvents("input_stream1", 10)
//      Thread.sleep(100)
//      //hope events make it to the destination
//      destinationStore.size should equal (10)
//
//      // write more events the ingestion stream
//      produceEvents("input_stream1", 10)
//      Thread.sleep(100)
//      //hope again we have the new events make it to the destination
//      destinationStore.size should equal (20)
//    }
//    finally{
//      cmShutdown()
//      shutdownTasks(EtherdEnv.get)
//    }
//   }
//
//   "A topology with multiple stages" should "process events and sink to destination output" in {
//    // start the cluster manager
//    val cmActor = ClusterManager.start()
//
//    // final destination sink
//    val destinationStore = buildDummyDestinationStream("final_destination")
//
//    // build the topology and run it.
//    val tp = Topology("topology2")
//    val spn = tp.ingest("input_stream1")
//      .map(e => Event(e.getKey, e.getRecord.getRaw, e.getOrder))
//      .dropByKeys(List("#baddata"))
//      .sink(buildPass)
//      .sink(new WritableStreamSpec("final_destination"))
//    val stageList = ListBuffer.empty[Stage]
//    tp.run()
//
//    // wait for executors to start and the topology to process events
//    Thread.sleep(1000)
//
//    try {
//      // ask the clusterManager if executor registrations are successful
//      val f = cmActor.ask(GetRegisteredExecutors("topology2"))(1 seconds).mapTo[ExecutorList]
//      val registeredExecutors = Await.result(f, 1 seconds)
//      registeredExecutors.executors.size should equal (2)
//
//      // write events into the ingestion stream
//      produceEvents("input_stream1", 10)
//      Thread.sleep(100)
//      //hope the events make it to the destination
//      destinationStore.size should equal (10)
//
//      // write more events the ingestion stream
//      produceEvents("input_stream1", 10)
//      Thread.sleep(100)
//
//      //hope again we have the new events make it to the destination
//      destinationStore.size should equal (20)
//    }
//    finally{
//      cmShutdown()
//      shutdownTasks(EtherdEnv.get)
//    }
//  }
//
//   "A topology with multiple stages each with 4 partitions" should "process events and sink to destination output" in {
//    // start the cluster manager
//    val cmActor = ClusterManager.start()
//
//     val destinationStore = new ConcurrentLinkedQueue[Event]()
//     subscribeDestinationToStream("final_destination", destinationStore, 0)
//     subscribeDestinationToStream("final_destination", destinationStore, 1)
//     subscribeDestinationToStream("final_destination", destinationStore, 2)
//     subscribeDestinationToStream("final_destination", destinationStore, 3)
//
//    // build the topology and run it.
//    val tp = Topology("topology3", 4)
//    val spn = tp.ingest("input_stream1")
//      .map(e => Event(e.getKey, e.getRecord.getRaw, e.getOrder))
//      .dropByKeys(List("#baddata"))
//      .sink(buildPass)
//      .sink(new WritableStreamSpec("final_destination"))
//    val stageList = ListBuffer.empty[Stage]
//    tp.run()
//
//    // wait for executors to start and the topology to process events
//    Thread.sleep(1000)
//
//    try {
//      // ask the clusterManager if executor registrations are successful
//      val f = cmActor.ask(GetRegisteredExecutors("topology3"))(1 seconds).mapTo[ExecutorList]
//      val registeredExecutors = Await.result(f, 1 seconds)
//      registeredExecutors.executors.size should equal (2 * 4) // stages * partitions
//
//      // write events into the ingestion stream
//      produceEvents("input_stream1", 10, 0)
//      produceEvents("input_stream1", 10, 1)
//      produceEvents("input_stream1", 10, 2)
//      produceEvents("input_stream1", 10, 3)
//      Thread.sleep(100)
//
//      //hope the events make it to the destination
//      destinationStore.size should equal (40)
//
//      // write more events the ingestion stream
//      produceEvents("input_stream1", 10)
//      Thread.sleep(100)
//
//      //hope again we have the new events make it to the destination
//      destinationStore.size should equal (50)
//    }
//    finally{
//      cmShutdown()
//      shutdownTasks(EtherdEnv.get)
//    }
//  }

  "2 topologies each with multiple stages" should "when run in parallel process events" in {
    // start the cluster manager
    val cmActor = ClusterManager.start()
    // final destination sink
    val destinationStore = buildDummyDestinationStream("final_destination")

    // build the topology and run it.
    val tp = Topology("topology4")
    tp.ingest("input_stream1")
      .map(e => Event(e.getKey, e.getRecord.getRaw, e.getOrder))
      .dropByKeys(List("#baddata"))
      .sink(buildPass("topology4"))
      .sink(new WritableStreamSpec("final_destination"))
    tp.run()

    // build another topology and run it.
    val tp2 = Topology("topology5")
    tp2.ingest("input_stream2")
      .map(e => Event(e.getKey, e.getRecord.getRaw, e.getOrder))
      .sink(buildPass("topology5"))
      .sink(new WritableStreamSpec("final_destination"))
    tp2.run()

    //wait for executors to start
    Thread.sleep(1000)

    try {
      // write events into the streams
      produceEvents("input_stream1", 15) // 15 events for topic input_stream1 on partition 0
      produceEvents("input_stream2", 12) // 12 events for topic input_stream2 on partition 0

	  // wait for events to propagate
      Thread.sleep(2000)

      destinationStore.size should equal (27)
    }
    finally{
      cmShutdown()
      shutdownTasks(EtherdEnv.get)
    }
  }
//
//   "2 topologies each with multiple stages and partitions" should "process events" in {
//    // start the cluster manager
//    val cmActor = ClusterManager.start()
//
//    // final destination sink
//    val destinationStore = new ConcurrentLinkedQueue[Event]()
//    subscribeDestinationToStream("final_destination", destinationStore, 0)
//    subscribeDestinationToStream("final_destination", destinationStore, 1)
//    subscribeDestinationToStream("final_destination", destinationStore, 2)
//    subscribeDestinationToStream("final_destination", destinationStore, 3)
//
//    // build the topology and run it.
//    val tp = Topology("topology6", 2)  // default partitions is 2
//    tp.ingest("input_stream1")
//      .map(e => Event(e.getKey, e.getRecord.getRaw, e.getOrder))
//      .dropByKeys(List("#baddata"))
//      .sink(buildPass("topology6"))
//      .sink(new WritableStreamSpec("final_destination"))
//    tp.run()
//
//    // build another topology and run it.
//    val tp2 = Topology("topology7", 5) // default partitions is 5
//    tp2.ingest(new ReadableStreamSpec("input_stream2"))
//      .map(e => Event.clone(e))
//      .sink(buildPass("topology7"))
//      .sink(new WritableStreamSpec("final_destination"))
//    tp2.run()
//
//    // build another topology and run it.
//    val tp3 = Topology("topology8") // default partitions is 1
//    tp3.ingest(new ReadableStreamSpec("input_stream3"))
//      .map(e => Event.clone(e))
//      .sink(buildPass("topology8"))
//      .sink(new WritableStreamSpec("final_destination"))
//    tp3.run()
//
//    //wait for all executors to start
//    Thread.sleep(1000)
//
//    try {
//      // write events into the streams
//      produceEvents("input_stream1", 15, 0) // 15 events for topic input_stream1 on partition 0
//      produceEvents("input_stream1", 15, 1) // 15 events for topic input_stream1 on partition 1
//      produceEvents("input_stream2", 12, 0) // 12 events for topic input_stream2 on partition 0
//      produceEvents("input_stream2", 13, 1) // 13 events for topic input_stream2 on partition 1
//      produceEvents("input_stream2", 14, 2) // 14 events for topic input_stream2 on partition 2
//      produceEvents("input_stream2", 15, 3) // 15 events for topic input_stream2 on partition 3
//
//      Thread.sleep(2000)
//
//      // both the topologies have a common sink
//      destinationStore.size should equal (84)
//    }
//    finally{
//      cmShutdown()
//      shutdownTasks(EtherdEnv.get)
//    }
//  }
}
