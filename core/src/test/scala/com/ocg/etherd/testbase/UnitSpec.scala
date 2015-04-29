package com.ocg.etherd.testbase

import java.util.concurrent.ConcurrentLinkedQueue

import com.ocg.etherd.messaging.{LocalWritableDMessageBusStream, LocalReadableDMessageBusStream, LocalDMessageBus}
import com.ocg.etherd.runtime.ClusterManager
import com.ocg.etherd.runtime.scheduler.{SchedulableTask, ResourceAsk}
import com.ocg.etherd.topology.Stage
import org.scalatest._
import com.ocg.etherd.streams._
import com.ocg.etherd.spn._
import com.ocg.etherd.EtherdEnv

/**
 *
 */
abstract class UnitSpec extends FlatSpec with Matchers with
OptionValues with Inside with Inspectors with BeforeAndAfterEachTestData
{
  def buildPass: SPN = EventOps.pass("topology")

  def buildFilter(filter: String): SPN = EventOps.dropByKeys("topology", List(filter))

  def buildFlatMap(f: Event => Iterator[Event]): SPN = EventOps.flatMap("topology", f)

  def buildDummyDestinationStream(topic: String): ConcurrentLinkedQueue[Event] = {
    val q = new ConcurrentLinkedQueue[Event]()
    val destinationStream = buildReadableEventStream(topic)
    destinationStream.subscribe((topic, event) => {
      q.add(event)
    })
    destinationStream.init(0)
    q
  }

  // streams
  def buildReadableEventStream(streamName: String): ReadableEventStream = {
    val factory = EtherdEnv.env.getStreamBuilder
    factory.buildReadableStream(new ReadableEventStreamSpec(streamName))
  }

  def buildLocalReadableStream(bus: LocalDMessageBus, streamName: String): LocalReadableDMessageBusStream = {
    bus.buildStream(streamName).asInstanceOf[LocalReadableDMessageBusStream]
  }

  def buildLocalWritableStream(bus: LocalDMessageBus, streamName: String): LocalWritableDMessageBusStream = {
    bus.buildWriteOnlyStream(streamName).asInstanceOf[LocalWritableDMessageBusStream]
  }

  def buildWritableEventStream(streamName: String): WritableEventStream = {
    val factory = EtherdEnv.env.getStreamBuilder
    factory.buildWritableStream(new WritableEventStreamSpec(streamName))
  }

  def buildUnitSchedulableTask: SchedulableTask[Int] = {
    buildUnitSchedulableTask[Int](1)
  }

  def buildUnitSchedulableTask[T](taskInfo: T): SchedulableTask[T] = {
    new SchedulableTask[T](taskInfo, new ResourceAsk(1, 1))
  }

  def getStageTasks(stage: Stage) = {
    stage.setStageId(1)
    stage.setTopologyId("topology")
    stage.setTopologyExecutionManagerActorUrl("")
    stage.buildTasks
  }

  // event generation
  def produceEvents(topic: String, numEvents: Int): Unit = {
    val wstream = EtherdEnv.get.getStreamBuilder.buildWritableStream(new WritableEventStreamSpec(topic))
    wstream.init(0)
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
    env.getScheduler.shutdownTasks()
  }


  override protected def beforeEach(testData: TestData): Unit = {
    EtherdEnv.rebuild()
  }
}
