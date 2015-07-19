package com.ocg.etherd.testbase

import java.util.concurrent.ConcurrentLinkedQueue

import com.ocg.etherd.messaging._
import com.ocg.etherd.runtime.ClusterManager
import com.ocg.etherd.runtime.RuntimeMessages.ShutdownAllScheduledTasks
import com.ocg.etherd.runtime.scheduler.{SchedulableTask, ResourceAsk}
import com.ocg.etherd.topology.{StageSchedulingInfo, Stage}
import org.scalatest._
import com.ocg.etherd.streams._
import com.ocg.etherd.spn._
import com.ocg.etherd.EtherdEnv

import scala.reflect.ClassTag

/**
 * Base class for Unit tests
 */
abstract class UnitSpec extends FlatSpec with Matchers with
OptionValues with Inside with Inspectors with BeforeAndAfterEachTestData
{
  def buildPass: SPN = this.buildPass("topology")

  def buildPass(topologyName: String): SPN = EventOps.pass(topologyName)

  def buildFilter(filter: String): SPN = this.buildFilter(filter, "topology")

  def buildFilter(filter: String, topology: String): SPN = EventOps.dropByKeys(topology, List(filter))

  def buildMap(f: Event => Event): SPN = this.buildMap(f, "topology")

  def buildMap(f: Event => Event, topology: String): SPN = EventOps.map(topology, f)

  def buildFlatMap(f: Event => Iterator[Event]): SPN = this.buildFlatMap(f, "topology")

  def buildFlatMap(f: Event => Iterator[Event], topology: String): SPN = EventOps.flatMap(topology, f)

  def buildDummyDestinationStream(topic: String, partition: Int = 0): ConcurrentLinkedQueue[Event] = {
    val destinationQueue = new ConcurrentLinkedQueue[Event]()
    this.subscribeDestinationToStream(topic, destinationQueue, partition)
    destinationQueue
  }

  def subscribeDestinationToStream(topic: String, destinationQueue: ConcurrentLinkedQueue[Event],
                                   partition: Int = 0): Unit = {
    val destinationStream = buildLocalReadableStream(topic)
    destinationStream.subscribe((topic, event) => {
      destinationQueue.add(event)
    })
    println(s"Initializing destination stream $topic for partition: $partition")
    destinationStream.init(partition)
  }

  def buildLocalReadableStream(streamName: String): LocalReadableStream = {
    new ReadableStreamSpec(streamName).buildReadableStream.asInstanceOf[LocalReadableStream]
  }

  def buildLocalWritableStream(streamName: String): LocalWritableStream = {
    new WritableStreamSpec(streamName).buildWritableStream.asInstanceOf[LocalWritableStream]
  }

  def buildUnitSchedulableTask: SchedulableTask[Int] = {
    buildUnitSchedulableTask[Int](1)
  }

  def buildUnitSchedulableTask[T: ClassTag](taskInfo: T): SchedulableTask[T] = {
    new SchedulableTask[T](taskInfo, new ResourceAsk(1, 1))
  }

  def getStageTasks(stage: Stage): Iterator[SchedulableTask[StageSchedulingInfo]] = {
    stage.setStageId(1)
    stage.setTopologyId("topology")
    stage.setTopologyExecutionManagerActorUrl("")
    stage.buildTasks
  }

  // event generation
  def produceEvents(topic: String, numEvents: Int, partition: Int = 0): Unit = {
    val wstream = buildLocalWritableStream(topic)
    wstream.init(partition)
    produceEvents(wstream, numEvents)
  }

  def produceEvents(wstream: WritableEventStream, numEvents: Int) = {
    val t = new Thread {
      override def run(): Unit ={
        for (i <- 0 until numEvents) {
          wstream.push(Event(i.toString, i, 0))
        }
      }
    }
    t.start()
  }

  def cmShutdown(): Unit = {
    ClusterManager.shutdown()
    // wait for the cluster manager to shutdown and release the port
    Thread.sleep(500)
  }

  def shutdownTasks(env: EtherdEnv): Unit = {
    env.getClusterManagerRef ! ShutdownAllScheduledTasks
  }

  override protected def beforeEach(testData: TestData): Unit = {
    EtherdEnv.rebuild()
  }
}
