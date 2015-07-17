package com.ocg.etherd.runtime


import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._
import com.ocg.etherd.topology.Stage
import com.ocg.etherd.EtherdEnv
import com.ocg.etherd.runtime.RuntimeMessages._
import com.ocg.etherd.testbase.UnitSpec
import com.typesafe.config.ConfigFactory
import akka.actor.{Actor, ActorRef, ActorSystem, ActorSelection}
import akka.actor.Props
import akka.pattern.ask
import akka.event.Logging

/**
 * Tests system actor behaviours. Should not focus on end to end execution.
 */
class ActorSpec extends UnitSpec {
  "ClusterManager" should "be able to start and respond to requests" in {
    // cluster manager system
    val cmActor = ClusterManager.start()
    try {
      val f = cmActor.ask(GetRegisteredExecutors("topology"))(1 seconds).mapTo[ExecutorList]
      val registeredExecutors = Await.result(f, 1 seconds)
      registeredExecutors.executors.size should equal(0)
    }
    finally {
      cmShutdown()
    }
  }

  "A single executor" should "register with topology execution manager on startup" in {
    val env = EtherdEnv.get

    // build stages
    val stageList = mutable.ListBuffer.empty[Stage]
    buildPass.buildStages(stageList)
    stageList.foreach { stage => stage.setDefaultPartitionSize(2) }
    stageList.toList.size should equal (1)

    // cluster manager system
    val cmActor = ClusterManager.start()
    try {
      // submit stages and wait for execution manager child actor to start
      // and executors to register with the execution manager
      cmActor ! SubmitStages("topology", stageList.toList)
      Thread.sleep(500)


      // ask the clusterManager if registrations happened for topology
      val f = cmActor.ask(GetRegisteredExecutors("topology"))(1 seconds).mapTo[ExecutorList]
      val registeredExecutors = Await.result(f, 1 seconds)
      registeredExecutors.executors.size should equal (1 * 2) // stages * partitions
    }
    finally {
      // shutdown clean
      println("shutdown executor systems")
      cmShutdown()
    }
  }

  "Multiple Executors" should "register with topology execution manager on startup" in {
    val env = EtherdEnv.get

    // build stages
    val stageList = mutable.ListBuffer.empty[Stage]
    val passThrough = buildPass
    passThrough.sink(buildFilter("#badata")).sink(buildPass)
    passThrough.buildStages(stageList)
    stageList.foreach { stage => stage.setDefaultPartitionSize(4) }

    stageList.toList.size should equal (3)

    // cluster manager system
    val cmActor = ClusterManager.start()
    try {
      // submit stages and wait for execution manager child actor to start
      // and executors to register with the execution manager
      cmActor ! SubmitStages("topology", stageList.toList)
      Thread.sleep(2000)


      // ask the clusterManager if registrations happened for topology
      val f = cmActor.ask(GetRegisteredExecutors("topology"))(1 seconds).mapTo[ExecutorList]
      val registeredExecutors = Await.result(f, 1 seconds)
      registeredExecutors.executors.size should equal (3 * 4) // stages * partitions
    }
    finally {
      // shutdown clean
      cmShutdown()
    }
  }

  "it" should "when spawned by multiple topologies should register with respective topology execution manager on startup" in {
    val env = EtherdEnv.get

    val stageList1 = mutable.ListBuffer.empty[Stage]
    val passThrough1 = buildPass
    passThrough1.sink(buildFilter("#badata"))
    passThrough1.buildStages(stageList1)

    val stageList2 = mutable.ListBuffer.empty[Stage]
    val passThrough2 = buildPass
    passThrough2.sink(buildFilter("#badata"))
    passThrough2.buildStages(stageList2)
    stageList2.foreach { stage => stage.setDefaultPartitionSize(2)}

    stageList1.toList.size should equal (2)
    stageList2.toList.size should equal (2)

    // cluster manager system
    val cmActor = ClusterManager.start()

    try {
      // submit stages and wait for execution manager child actor to start
      // and executors to register with the execution manager
      cmActor ! SubmitStages("topology1", stageList1.toList)
      cmActor ! SubmitStages("topology2", stageList2.toList)
      Thread.sleep(2000)


      // ask the clusterManager if registrations happened for topology
      var f = cmActor.ask(GetRegisteredExecutors("topology1"))(1 seconds).mapTo[ExecutorList]
      var registeredExecutors = Await.result(f, 1 seconds)
      registeredExecutors.executors.size should equal (2 * 1) // stages * partitions

      // ask the clusterManager if registrations happened for topology
      f = cmActor.ask(GetRegisteredExecutors("topology2"))(1 seconds).mapTo[ExecutorList]
      registeredExecutors = Await.result(f, 1 seconds)
      registeredExecutors.executors.size should equal (2 * 2) // stages * partitions
    }
    finally {
      // shutdown clean
      cmShutdown()
    }
  }
}
