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
      registeredExecutors.executors.size should equal(0)
    }
    finally {
      cmShutdown()
    }
  }

  "A single executor" should "register with cluster manager on startup" in {
    val env = EtherdEnv.get

    // build stages
    val stageList = mutable.ListBuffer.empty[Stage]
    new PassThroughSPN("topology").buildStages(stageList)

    stageList.toList.size should equal (1)

    // cluster manager system
    val cmActor = ClusterManager.start()
    // submit stages and wait for execution manager child actor to start
    // and executors to register with the execution manager
    cmActor ! SubmitStages("topology", stageList.toList)
    Thread.sleep(500)

    try {
      // ask the clusterManager if registrations happened for topology
      val f = cmActor.ask(GetRegisteredExecutors("topology"))(1 seconds).mapTo[ExecutorList]
      val registeredExecutors = Await.result(f, 1 seconds)
      registeredExecutors.executors.size should equal (1)
    }
    finally {
      // shutdown clean
      println("shutdown executor systems")
      cmShutdown()
      shutdownTasks(env)
    }
  }

  "Multiple Executors" should "register with cluster manager on startup" in {
    val env = EtherdEnv.get

    // build stages
    val stageList = mutable.ListBuffer.empty[Stage]
    val passThrough = new PassThroughSPN("topology")
    passThrough.sink(new FilterKeysSPN("topology", List("#badata")))
    passThrough.buildStages(stageList)

    stageList.toList.size should equal (2)

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
      registeredExecutors.executors.size should equal (2)
    }
    finally {
      // shutdown clean
      cmShutdown()
      shutdownTasks(env)
    }
  }

  "it" should "when spawned by multiple topologies should register with cluster manager on startup" in {
    val env = EtherdEnv.get

    val stageList1 = mutable.ListBuffer.empty[Stage]
    val passThrough1 = new PassThroughSPN("topology")
    passThrough1.sink(new FilterKeysSPN("topology", List("#badata")))
    passThrough1.buildStages(stageList1)

    val stageList2 = mutable.ListBuffer.empty[Stage]
    val passThrough2 = new PassThroughSPN("topology1")
    passThrough2.sink(new FilterKeysSPN("topology1", List("#badata")))
    passThrough2.buildStages(stageList2)

    stageList1.toList.size should equal (2)
    stageList2.toList.size should equal (2)

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
      registeredExecutors.executors.size should equal (2)

      // ask the clusterManager if registrations happened for topology
      f = cmActor.ask(GetRegisteredExecutors("topology1"))(1 seconds).mapTo[ExecutorList]
      registeredExecutors = Await.result(f, 1 seconds)
      registeredExecutors.executors.size should equal (2)
    }
    finally {
      // shutdown clean
      cmShutdown()
      shutdownTasks(env)
    }
  }
}
