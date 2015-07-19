package com.ocg.etherd

import akka.actor.{ActorSystem, ActorSelection}
import com.ocg.etherd.runtime.akkautils.Utils
import com.ocg.etherd.messaging.{DMessageBus, LocalDMessageBus}
import com.ocg.etherd.runtime.ClusterManager
import com.ocg.etherd.runtime.scheduler.{Scheduler, LocalScheduler}

import scala.util.Random

class EtherdEnv(configuration: EtherdConf) {
  var defaultMessageBus = this.resolveDefaultMessageBus
  var tpClientActorSystem: Option[ActorSystem] = None

  def getConfiguration: EtherdConf = this.configuration

  def getDefaultMessageBus: DMessageBus = this.defaultMessageBus

  def getClusterManagerRef: ActorSelection = synchronized {
    this.tpClientActorSystem match {
      case Some(actorSystem) =>  actorSystem.actorSelection(ClusterManager.clusterManagerActorUrl)
      case None =>  {
        this.tpClientActorSystem = Some(buildActorSystem)
        this.tpClientActorSystem.get.actorSelection(ClusterManager.clusterManagerActorUrl)
      }
    }
  }

  private def resolveDefaultMessageBus = {
    new LocalDMessageBus()
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
