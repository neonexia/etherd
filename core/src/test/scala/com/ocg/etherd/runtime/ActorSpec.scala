package com.ocg.etherd.runtime


import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._
import com.ocg.etherd.topology.Stage
import com.ocg.etherd.{ActorUtils, EtherdEnv}
import com.ocg.etherd.runtime.RuntimeMessages._
import com.ocg.etherd.testbase.UnitSpec
import com.typesafe.config.ConfigFactory
import akka.actor.{Actor, ActorRef, ActorSystem, ActorSelection}
import akka.actor.Props
import akka.pattern.ask
import akka.event.Logging
import com.ocg.etherd.spn.{FilterKeysSPN, PassThroughSPN}

class ActorSpec extends UnitSpec {
  "ClusterManager" should "be able to start and respond to requests" in {
    // cluster manager system
    val cmActor = ClusterManager.start()
    try {
      val f = cmActor.ask(GetRegisteredExecutors("topology"))(1 seconds).mapTo[ExecutorList]
      val registeredExecutors = Await.result(f, 1 seconds)
      assertResult(0) {
        registeredExecutors.executors.size
      }
    }
    finally {
      ClusterManager.shutdown()
    }
  }

  "A single executor" should "register with cluster manager on startup" in {
    // build stages
    val stageList = mutable.ListBuffer.empty[Stage]
    new PassThroughSPN("topology").buildStages(stageList)

    assertResult(1) {
        stageList.toList.size
    }

    // cluster manager system
    val cmActor = ClusterManager.start()
    // submit stages and wait for execution manager child actor to start
    // and executors to register with the execution manager
    cmActor ! SubmitStages("topology", stageList.toList)
    Thread.sleep(2000)

    try {
      // ask the clusterManager if registrations happened for topology
      val f = cmActor.ask(GetRegisteredExecutors("topology"))(1 seconds).mapTo[ExecutorList]
      val registeredExecutors = Await.result(f, 1 seconds)
      assertResult(1) {
        registeredExecutors.executors.size
      }
    }
    finally {
      // shutdown clean
      ClusterManager.shutdown()
      Executor.shutdown()
    }
  }

  "Multiple Executors" should "register with cluster manager on startup" in {
    // build stages
    val stageList = mutable.ListBuffer.empty[Stage]
    val filterSPN = new FilterKeysSPN("topology", List("#badata"))
    val passThrough = new PassThroughSPN("topology")
    passThrough.sink(List(filterSPN))
    passThrough.buildStages(stageList)

    assertResult(2) {
      stageList.toList.size
    }

    // cluster manager system
    val cmActor = ClusterManager.start()
    // submit stages and wait for execution manager child actor to start
    // and executors to register with the execution manager
    cmActor ! SubmitStages("topology", stageList.toList)
    Thread.sleep(2000)

    try {
      // ask the clusterManager if registrations happened for topology
      val f = cmActor.ask(GetRegisteredExecutors("topology"))(1 seconds).mapTo[ExecutorList]
      val registeredExecutors = Await.result(f, 1 seconds)
      assertResult(2) {
        registeredExecutors.executors.size
      }
    }
    finally {
      // shutdown clean
      ClusterManager.shutdown()
      Executor.shutdown()
    }
  }

  "it" should "when spawned by multiple topologies register with cluster manager on startup" in {
    val stageList1 = mutable.ListBuffer.empty[Stage]
    val stageList2 = mutable.ListBuffer.empty[Stage]
    val filterSPN = new FilterKeysSPN("topology", List("#badata"))
    val passThrough = new PassThroughSPN("topology")
    passThrough.sink(List(filterSPN))
    passThrough.buildStages(stageList1)
    passThrough.buildStages(stageList2)

    assertResult(2) {
      stageList1.toList.size
    }

    assertResult(2) {
      stageList2.toList.size
    }

    // cluster manager system
    val cmActor = ClusterManager.start()

    // submit stages and wait for execution manager child actor to start
    // and executors to register with the execution manager
    cmActor ! SubmitStages("topology", stageList1.toList)
    cmActor ! SubmitStages("topology1", stageList2.toList)
    Thread.sleep(2000)

    try {
      // ask the clusterManager if registrations happened for topology
      var f = cmActor.ask(GetRegisteredExecutors("topology"))(1 seconds).mapTo[ExecutorList]
      var registeredExecutors = Await.result(f, 1 seconds)
      assertResult(2) {
        registeredExecutors.executors.size
      }

      // ask the clusterManager if registrations happened for topology
      f = cmActor.ask(GetRegisteredExecutors("topology1"))(1 seconds).mapTo[ExecutorList]
      registeredExecutors = Await.result(f, 1 seconds)
      assertResult(2) {
        registeredExecutors.executors.size
      }
    }
    finally {
      // shutdown clean
      ClusterManager.shutdown()
      Executor.shutdown()
    }
  }
}
