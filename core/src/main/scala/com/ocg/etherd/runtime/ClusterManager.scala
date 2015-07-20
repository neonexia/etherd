package com.ocg.etherd.runtime

import scala.concurrent.Await
import scala.concurrent.duration._
import akka.actor.{Actor, ActorRef, ActorSystem}
import akka.pattern.ask
import com.ocg.etherd.runtime.akkautils.Utils
import com.ocg.etherd.{Logging, EtherdEnv}
import com.ocg.etherd.runtime.RuntimeMessages._
import com.ocg.etherd.topology.Stage

import scala.collection.mutable

/**
 * ClusterManager runs as a master daemon on the cluster and acts as the primary interface for etherd clients
 * 1. Loads configuration and initializes services like scheduler, message bus, state manager, eventing, logging etc.
 * 2. Receives topology specifications from clients and submits them to the topology execution manager
 * 3. Responds to requests for topology runtime status and cluster health
 * @param clusterManagerActorUrlBase
 */
private[etherd] class ClusterManager(clusterManagerActorUrlBase: String) extends Actor with Logging{
  val topologyManagersMap = mutable.HashMap.empty[String, ActorRef]
  val schedulerActorRef = Utils.buildSchedulerActor(this.context)

  def receive = testHooksReceive orElse runtimeReceive

  def runtimeReceive: Receive = {
    case SubmitStages(topologyName: String, stages: List[Stage]) => {
      logInfo(s"Received SubmitStages for topology $topologyName")
      this.topologyManagersMap.get(topologyName) match {
        case Some(actorRef) => {
          sender ! logDebug("topology already executing. Ignoring request")
        }
        case None => {
          try {
            logDebug(s"Received Submit Stages for topology $topologyName")
            val actorName = s"topologyExecutionManagerActor_$topologyName"
            val executionManagerActorUrl = s"$clusterManagerActorUrlBase/executionManagerActor_$topologyName"
            val executionManagerActor = Utils.buildExecutionManagerActor(context, topologyName, executionManagerActorUrl,
                                                                         s"executionManagerActor_$topologyName",
                                                                         this.schedulerActorRef)
            topologyManagersMap += topologyName -> executionManagerActor
            executionManagerActor ! ScheduleStages(stages)
          }
          catch {
            case e:Exception => {
              logError("Exception creating topology execution manager", e)
            }
          }
        }
      }
    }
  }

  def testHooksReceive: Receive = {
    case ShutdownTopology(topologyName: String) => {
      // if we have a tem for this topology then start the shutdown process
      this.topologyManagersMap.get(topologyName).map {
        this.topologyManagersMap -= topologyName
        actorRef => this.context.stop(actorRef)
      }
    }
    case GetRegisteredExecutors(topologyName: String) => {
      logDebug(s"Received message GetRegisteredExecutors for topology $topologyName")
      this.topologyManagersMap.get(topologyName) match {
        case Some(actorRef) => {
          logInfo("await result from executionActor")
          sender ! Await.result(actorRef.ask(GetRegisteredExecutors(topologyName))(1 seconds).mapTo[ExecutorList], 1 seconds)
        }
        case None => {
          sender ! ExecutorList(List[ExecutorData]())
        }
      }
    }
    case ShutdownAllScheduledTasks() => {
      this.schedulerActorRef ! ShutdownAllScheduledTasks
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
    cmSystem = Some(Utils.buildActorSystem(s"$clusterManagerSystemName", cmSystemPort))
    Utils.buildClusterManagerActor(cmSystem.get, clusterManagerActorUrlBase, clusterManagerRootActorName)
  }

  def shutdown() = {
    cmSystem.map { actorSystem =>
      actorSystem.shutdown()
    }
  }
}
