package com.ocg.etherd.SchedulerSpec

import com.ocg.etherd.messaging.LocalDMessageQueue
import com.ocg.etherd.scheduler.{ClusterResource}
import com.ocg.etherd.spn.SPN
import com.ocg.etherd.testbase.UnitSpec
import com.ocg.etherd.topology.{Topology}

/**
*/
class SchedulerSpec extends UnitSpec {

  "A Scheduler" should "should return a 1 match with a 1 stage" in {
    // create an in stream
    val mq = new LocalDMessageQueue()
    val istream1 = mq.getEventQueue("defaultIn")

    val tp = new Topology("etherd.default")
    tp.ingest(List(istream1).iterator)
    tp.run()

    val r = ClusterResource(1,1)
    println(r.getCores)
    val matches = tp.scheduler.consumeOfferedResources(r)

    assertResult(1) {
      matches.size
    }

    assertResult(0) {
      tp.scheduler.getPendingTasks.size
    }
  }

  it should "return 3 matches when combined with sink stages and when offered a resource of 4 cores and 2GB RAM" in {
    // create an in stream
    val mq = new LocalDMessageQueue()
    val istream1 = mq.getEventQueue("defaultIn")

    // create a topology
    val tp = new Topology("etherd.default")
    val ingestspn = tp.ingest(List(istream1).iterator)

    // create 2 sink spns
    val spn1 = ingest()
    val spn2 = ingest()
    ingestspn.sink(List[SPN](spn1, spn2))

    // run the topology
    tp.run()
    val matches = tp.scheduler.consumeOfferedResources(ClusterResource(1,1))

    // should get 3 schedulable tasks
    assertResult(1) {
      matches.size
    }

    assertResult(2) {
      tp.scheduler.getPendingTasks.size
    }

    val r = ClusterResource(4,2)

    tp.scheduler.consumeOfferedResources(r)
    tp.scheduler.consumeOfferedResources(r)
    assertResult(0) {
      tp.scheduler.getPendingTasks.size
    }

    assertResult(2) {
      r.getCores
    }

    assertResult(0) {
      r.getMemory
    }
  }

  it should "not return more tasks than what can be scheduled with available resources" in {
    // create an in stream
    val mq = new LocalDMessageQueue()
    val istream1 = mq.getEventQueue("defaultIn")

    // create a topology
    val tp = new Topology("etherd.default")
    val ingestspn = tp.ingest(List(istream1).iterator)

    // create 3 sink spns
    val spn1 = ingest()
    val spn2 = ingest()
    val spn3 = ingest()
    ingestspn.sink(List[SPN](spn1, spn2, spn3))

    // run the topology
    tp.run()
    assertResult(4) {
      tp.scheduler.getPendingTasks.size
    }

    // offer resources that should be all consumed and available resources should be empty
    val r = ClusterResource(2,2)
    val matches = tp.scheduler.consumeOfferedResources(r)

    assertResult(2) {
      matches.size
    }

    assertResult(2) {
      tp.scheduler.getPendingTasks.size
    }

    assertResult(0) {
      r.getCores
    }

    assertResult(0) {
      r.getMemory
    }
  }
}
