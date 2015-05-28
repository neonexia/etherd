package com.ocg.etherd.spn

import java.util.concurrent.ConcurrentLinkedQueue

import com.ocg.etherd.runtime.scheduler.SchedulableTask
import com.ocg.etherd.testbase.UnitSpec
import com.ocg.etherd.streams._
import com.ocg.etherd.topology.{StageSchedulingInfo, Stage}
import scala.collection.mutable

/**
 */
class SPNSpec extends UnitSpec {
  "A pass through spn" should "pass all events from an input stream to output stream" in {
    // final destination sink
    val destinationStore = buildDummyDestinationStream("final_destination")

    // create a pass spn and configure it to send the events to the final_destination
    val pass = buildPass
    pass.attachInputStreamSpec(new ReadableStreamSpec("input_stream1"))
    pass.attachExternalOutputStreamSpec(new WritableStreamSpec("final_destination"))
    pass.initialize(0)

    //simulate some events on the input stream
    produceEvents("input_stream1", 10)
    Thread.sleep(500)

    // test if the events made it to the destination
    destinationStore.size should equal (10)
  }

  it should "when ingested with 2 streams should pass all events to output stream" in {
    // final destination sink
    val destinationStore = buildDummyDestinationStream("final_destination")

    // create a pass spn and configure it to send the events to the final_destination
    val pass = buildPass
    pass.attachInputStreamSpec(new ReadableStreamSpec("input_stream1"))
    pass.attachInputStreamSpec(new ReadableStreamSpec("input_stream2"))
    pass.attachExternalOutputStreamSpec(new WritableStreamSpec("final_destination"))
    pass.initialize(0)

    //simulate some events on the input stream
    produceEvents("input_stream1", 10)
    produceEvents("input_stream2", 10)
    Thread.sleep(500)

    // test if the events made it to the destination
    destinationStore.size should equal (20)
  }

  it should "when combined with 2 sinked Passthrough  SPN's should fan out all events to both the SPN's" in {
    // final destination sink
    val destinationStore = buildDummyDestinationStream("final_destination")

    // create a pass spn and configure it to send the events to the final_destination
    val pass = buildPass
    pass.attachInputStreamSpec(new ReadableStreamSpec("input_stream1"))
    pass.attachInputStreamSpec(new ReadableStreamSpec("input_stream2"))

    val pass2 = buildPass
    val pass3 = buildPass
    pass2.attachExternalOutputStreamSpec(new WritableStreamSpec("final_destination"))
    pass3.attachExternalOutputStreamSpec(new WritableStreamSpec("final_destination"))
    pass.split(List(pass2, pass3).iterator)

    // call begin when all is setup
    pass.initialize(0)
    pass2.initialize(0)
    pass3.initialize(0)

    //simulate some events on the input stream
    produceEvents("input_stream1", 10)
    produceEvents("input_stream2", 10)
    Thread.sleep(500)

    // test if the events made it to the destination
    destinationStore.size should equal (40)
  }

  it should "when combined with a sinked filter SPN's should filter out events to the final destination" in {
    // final destination sink
    val destinationStore = buildDummyDestinationStream("final_destination")

    // create a pass spn and configure it to send the events to the final_destination
    val pass = buildPass
    pass.attachInputStreamSpec(new ReadableStreamSpec("input_stream1"))
    pass.attachInputStreamSpec(new ReadableStreamSpec("input_stream2"))

    val pass2 = buildPass
    val filter = buildFilter("5")
    pass2.attachExternalOutputStreamSpec(new WritableStreamSpec("final_destination"))
    filter.attachExternalOutputStreamSpec(new WritableStreamSpec("final_destination"))
    pass.split(List(pass2, filter).iterator)

    // call begin when all is setup
    pass.initialize(0)
    pass2.initialize(0)
    filter.initialize(0)

    //simulate some events on the input stream
    produceEvents("input_stream1", 10)
    produceEvents("input_stream2", 10)
    Thread.sleep(500)

    // test if the events made it to the destination
    destinationStore.size should equal (38)
  }

  "A 2 level chain with ingest/map-filter-filter-destination" should "pass filtered events" in {
    // final destination sink
    val destinationStore = buildDummyDestinationStream("final_destination")

    // create a pass spn and configure it to send the events to the final_destination
    val ingestion = buildPass
    val firstFilter = buildFilter("2")
    val filterLast = buildFilter("5")
    val flatMap = ingestion.flatMap(ev => List(ev, ev).iterator)
    flatMap.split(List(firstFilter).iterator)
    firstFilter.split(List(filterLast).iterator)


    ingestion.attachInputStreamSpec(new ReadableStreamSpec("input_stream1"))
    filterLast.attachExternalOutputStreamSpec(new WritableStreamSpec("final_destination"))

    // call begin processing on all staged streams
    ingestion.initialize(0)
    firstFilter.initialize(0)
    filterLast.initialize(0)

    //simulate some events on the input stream
    produceEvents("input_stream1", 10)
    Thread.sleep(500)

    // test if the events made it to the destination
    destinationStore.size should equal (16)
  }

  "Linked SPN's" should "build a only 1 stage" in {
    val ingestion = buildPass
    val flatMap = ingestion.flatMap ( ev => List(ev, ev).iterator)

    var finalStageList = mutable.ListBuffer.empty[Stage]
    ingestion.buildStages(finalStageList)
    finalStageList.size should equal (1)
    finalStageList.foreach {stage =>
      getStageTasks(stage).size should equal (1)
    }
  }

  it should "when chained with 2 sinks should produce 3 stages" in {
    // create a pass spn and configure it to send the events to the final_destination
    val ingestion = buildPass
    val firstFilter = buildFilter("2")
    val filterLast = buildFilter("5")
    val flatMap = ingestion.flatMap(ev => List(ev, ev).iterator)
    flatMap.split(List(firstFilter).iterator)
    firstFilter.split(List(filterLast)iterator)

    var finalStageList = mutable.ListBuffer.empty[Stage]
    ingestion.buildStages(finalStageList)
    finalStageList.size should equal (3)
    finalStageList.foreach { stage =>
      stage.setDefaultPartitionSize(2)
      val tasks = getStageTasks(stage).toList
      tasks.size should equal (2)
      tasks.head.getTaskInfo[StageSchedulingInfo].partition should equal (0)
      tasks(1).getTaskInfo[StageSchedulingInfo].partition should equal (1)
    }
  }

  it should "fanned out to 2 sinks should produce 3 stages" in {
    // create a pass spn and configure it to send the events to the final_destination
    val ingestion = buildPass
    val firstFilter = buildFilter("2")
    val filterLast = buildFilter("5")
    val flatMap = ingestion.flatMap(ev => List(ev, ev).iterator)
    flatMap.split(List(firstFilter, filterLast).iterator)

    var finalStageList = mutable.ListBuffer.empty[Stage]
    ingestion.buildStages(finalStageList)
    finalStageList.size should equal (3)
    var stageNum = 0
    finalStageList.foreach { stage =>
      if (stageNum > 1) {
        stage.setDefaultPartitionSize(2)
        getStageTasks(stage).size should equal(2)
      }
      else
      {
        stage.setDefaultPartitionSize(4)
        getStageTasks(stage).size should equal(4)
      }
      stageNum += 1
    }
  }

  it should "with ingest/map/filter-filter-filter-sink produce 3 stages" in {
    // create a pass spn and configure it to send the events to the final_destination
    val ingestion = buildPass
    ingestion.flatMap(ev => List(ev, ev).iterator)
      .dropByKeys(List("#baddata"))   //ingest/flatMap/filter
      .sink(buildFilter("2")) //- filter
      .sink(buildFilter("5")) // -filter

    var finalStageList = mutable.ListBuffer.empty[Stage]
    ingestion.buildStages(finalStageList)
    finalStageList.size should equal (3)
    finalStageList.foreach {stage =>
      stage.setDefaultPartitionSize(0)
      getStageTasks(stage).size should equal (1)
    }
  }
}