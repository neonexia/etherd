package com.ocg.etherd.runtime

import java.util.concurrent.atomic.AtomicInteger
import com.ocg.etherd.runtime.scheduler.SchedulableTask

import scala.collection.mutable.ListBuffer
import scala.concurrent.Await
import scala.concurrent.duration._
import com.typesafe.config.ConfigFactory
import akka.actor.{Actor, ActorRef, ActorSystem, ActorSelection}
import akka.actor.Props
import akka.pattern.ask
import akka.event.Logging
import com.ocg.etherd.{ActorUtils, EtherdEnv}
import com.ocg.etherd.runtime.RuntimeMessages._
import com.ocg.etherd.topology.Stage

import scala.collection.mutable
import scala.util.Random

class ClusterManager(clusterManagerActorUrlBase: String) extends Actor {
  val log = Logging(context.system, this)
  val topologyManagersMap = mutable.HashMap.empty[String, ActorRef]

  def receive = {
    case SubmitStages(topologyName: String, stages: List[Stage]) => synchronized {
      println("Received Message SubmitStages")
      this.topologyManagersMap.get(topologyName) match {
        case Some(actorRef) => {
          log.info("topology already executing. Ignoring request")
        }
        case None => {
          try {
            log.info(s"Received Submit Stages for topology $topologyName")
            val actorName = s"topologyExecutionManagerActor_$topologyName"
            val executionManagerActorUrl = s"$clusterManagerActorUrlBase/executionManagerActor_$topologyName"
            val executionManagerActor = ActorUtils.buildExecutionManagerActor(context, topologyName, executionManagerActorUrl, s"executionManagerActor_$topologyName")
            topologyManagersMap += topologyName -> executionManagerActor
            executionManagerActor ! ScheduleStages(stages)
          }
          catch {
            case e:Exception => {
              log.error(e, "Exception creating topology execution manager")
            }
          }
        }
      }
    }
    case GetRegisteredExecutors(topologyName: String) => {
      println(s"Received message GetRegisteredExecutors for topology $topologyName")
      this.topologyManagersMap.get(topologyName) match {
        case Some(actorRef) => {
          log.info("await result from executionActor")
          sender ! Await.result(actorRef.ask(GetRegisteredExecutors(topologyName))(1 seconds).mapTo[ExecutorList], 1 seconds)
        }
        case None => {
          sender ! ExecutorList(List[ExecutorData]())
        }
      }
    }
  }
}

object ClusterManager {
  val hostname = java.net.InetAddress.getLocalHost.getCanonicalHostName
  val clusterManagerSystemName = "clusterManagerSystem"
  val clusterManagerRootActorName = "etherdClusterManager"
  val clusterManagerHost = "127.0.0.1"
  val cmSystemPort = 8181
  val clusterManagerActorUrlBase = s"akka.tcp://$clusterManagerSystemName@$clusterManagerHost:$cmSystemPort/user/$clusterManagerRootActorName"
  var cmSystem: Option[ActorSystem]= None

  def clusterManagerActorUrl = {
    clusterManagerActorUrlBase
  }

  def start(): ActorRef = synchronized {
    cmSystem = Some(ActorUtils.buildActorSystem(s"$clusterManagerSystemName", cmSystemPort))
    ActorUtils.buildClusterManagerActor(cmSystem.get, clusterManagerActorUrlBase, clusterManagerRootActorName)
  }

  def shutdown() = {
    cmSystem.map { actorSystem =>
      actorSystem.shutdown()
    }
  }
}