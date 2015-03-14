package com.ocg.etherd.runtime.scheduler

import com.ocg.etherd.EtherdEnv
import com.ocg.etherd.runtime.scheduler.ClusterResource
import com.ocg.etherd.testbase.UnitSpec

/**
*/
class LocalSchedulerSpec extends UnitSpec {

  "A Scheduler" should "should should return the pending tasks equal to scheduled" in {
    val scheduler = new LocalScheduler()
    scheduler.getPendingTasks.size should equal (0)
  }
//
//  it should "return 3 matches when combined with sink stages and when offered a resource of 4 cores and 2GB RAM" in {
//    val matches = tp.scheduler.consumeOfferedResources(ClusterResource(1,1))
//    assertResult(1) {
//      matches.size
//    }
//
//    assertResult(2) {
//      tp.scheduler.getPendingTasks.size
//    }
//
//    val r = ClusterResource(4,2)
//
//    tp.scheduler.consumeOfferedResources(r)
//    tp.scheduler.consumeOfferedResources(r)
//    assertResult(0) {
//      tp.scheduler.getPendingTasks.size
//    }
//
//    assertResult(2) {
//      r.getCores
//    }
//
//    assertResult(0) {
//      r.getMemory
//    }
//  }
//
//  it should "not return more tasks than what can be scheduled with available resources" in {
//    // create an in stream
//    val mq = new LocalDMessageBus()
//    val istream1 = mq.getEventQueue("defaultIn")
//
//    // create a topology
//    val tp = new Topology("etherd.default")
//    val ingestspn = tp.ingest(List(istream1).iterator)
//
//    // create 3 sink spns
//    val spn1 = ingest()
//    val spn2 = ingest()
//    val spn3 = ingest()
//    ingestspn.sink(List[SPN](spn1, spn2, spn3))
//
//    // run the topology
//    tp.run()
//    assertResult(4) {
//      tp.scheduler.getPendingTasks.size
//    }
//
//    // offer resources that should be all consumed and available resources should be empty
//    val r = ClusterResource(2,2)
//    val matches = tp.scheduler.consumeOfferedResources(r)
//
//    assertResult(2) {
//      matches.size
//    }
//
//    assertResult(2) {
//      tp.scheduler.getPendingTasks.size
//    }
//
//    assertResult(0) {
//      r.getCores
//    }
//
//    assertResult(0) {
//      r.getMemory
//    }
//  }
}
