package com.ocg.etherd

import akka.actor.{ActorSystem, ActorSelection}
import com.ocg.etherd.runtime.akkautils.Utils
import com.ocg.etherd.messaging.{LocalDMessageBusStreamBuilder, DMessageBus, LocalDMessageBus}
import com.ocg.etherd.runtime.ClusterManager
import com.ocg.etherd.runtime.scheduler.{Scheduler, LocalScheduler}
import com.ocg.etherd.streams.EventStreamBuilder

import scala.util.Random

class EtherdEnv(configuration: EtherdConf) {
  val scheduler = this.resolveScheduler
  var streamBuilder = this.resolveStreamBuilder
  var tpClientActorSystem:Option[ActorSystem] = None

  def getConfiguration: EtherdConf = this.configuration

  def getScheduler: Scheduler = this.scheduler

  def getStreamBuilder: EventStreamBuilder = this.streamBuilder

  def getClusterManagerRef: ActorSelection = synchronized {
    println("Cluster Manager URL:" + ClusterManager.clusterManagerActorUrl)
    this.tpClientActorSystem match {
      case Some(actorSystem) =>  actorSystem.actorSelection(ClusterManager.clusterManagerActorUrl)
      case None =>  {
        this.tpClientActorSystem = Some(buildActorSystem)
        this.tpClientActorSystem.get.actorSelection(ClusterManager.clusterManagerActorUrl)
      }
    }
  }

  private def resolveScheduler = {
    new LocalScheduler()
  }

  private def resolveStreamBuilder = {
    new LocalDMessageBusStreamBuilder("local")
  }

  private def buildActorSystem = {
    Utils.buildActorSystem("etherdClient", 7000 + Random.nextInt(500))
  }
}

object EtherdEnv {
  var env:EtherdEnv = build

  def get = {
    env
  }

  private[etherd] def rebuild():Unit = {
    env = build
  }

  private def build: EtherdEnv = {
    new EtherdEnv(new EtherdConf())
  }
}
