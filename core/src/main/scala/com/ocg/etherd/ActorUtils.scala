package com.ocg.etherd

import com.ocg.etherd.runtime.{Executor, ClusterManager, TopologyExecutionManager}
import com.ocg.etherd.topology.Stage
import com.typesafe.config.{Config, ConfigFactory}
import akka.actor._
import akka.event.Logging

object ActorUtils {

  def buildActorSystem(systemName: String, port: Int): ActorSystem = {
    println(s"buildActorSystem with name $systemName")
    val confFile = new java.io.File(getClass.getResource("/application.conf").getPath)
    val config = ConfigFactory.parseString(s"akka.remote.netty.tcp.port=$port").withFallback(ConfigFactory.parseFile(confFile))
    ActorSystem(systemName, config)
  }

  def buildClusterManagerActor(cmSystem: ActorSystem, clusterManagerActorUrlBase:String, actorName: String): ActorRef = {
    cmSystem.actorOf(Props(new ClusterManager(clusterManagerActorUrlBase)), name=actorName)
  }

  def buildExecutionManagerActor(context: ActorContext, topologyName: String, clusterManagerActorUrl: String, actorName: String): ActorRef = {
    context.actorOf(Props(TopologyExecutionManager(topologyName, clusterManagerActorUrl)), name=actorName)
  }

  def buildExecutorActor(executorId: String, host:String, port: Int,
                         exSystem: ActorSystem, topologyName: String,
                         topologyExecutionManagerActorUrl: String, actorName: String): ActorRef = {
    println(s"buildExecutorActor: Creating an executor actor $executorId")
    exSystem.actorOf(Props(Executor(executorId, host, port, topologyName, exSystem.actorSelection(topologyExecutionManagerActorUrl))), name = actorName)
  }
}
