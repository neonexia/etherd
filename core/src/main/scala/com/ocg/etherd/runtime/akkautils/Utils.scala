package com.ocg.etherd.runtime.akkautils

import akka.actor._
import com.ocg.etherd.Logging
import com.ocg.etherd.runtime.executor.{ExecutorWorker, Executor}
import com.ocg.etherd.runtime.{StageExecutionContext, ClusterManager, TopologyExecutionManager}
import com.ocg.etherd.topology.StageSchedulingInfo
import com.typesafe.config.ConfigFactory

import scala.concurrent.Await
import scala.concurrent.duration._

object Utils extends Logging{

  def buildActorSystem(systemName: String, port: Int): ActorSystem = {
    logInfo(s"buildActorSystem with name $systemName")
    val confFile = new java.io.File(getClass.getResource("/application.conf").getPath)
    val config = ConfigFactory.parseString(s"akka.remote.netty.tcp.port=$port").withFallback(ConfigFactory.parseFile(confFile))
    ActorSystem(systemName, config)
  }

  def buildClusterManagerActor(cmSystem: ActorSystem, clusterManagerActorUrlBase:String, actorName: String): ActorRef = {
    cmSystem.actorOf(Props(new ClusterManager(clusterManagerActorUrlBase)), name=actorName)
  }

  def buildExecutionManagerActor(context: ActorContext, topologyName: String, clusterManagerActorUrl: String, actorName: String): ActorRef = {
    context.actorOf(Props(new TopologyExecutionManager(topologyName, clusterManagerActorUrl)), name=actorName)
  }

  def buildExecutorActor(exSystem: ActorSystem, executorId: String, stageSchedulingInfo: StageSchedulingInfo, host:String, port: Int): ActorRef = {
    logInfo(s"buildExecutorActor: Creating supervisor executor actor $executorId")
    exSystem.actorOf(Props(new Executor(executorId, stageSchedulingInfo:StageSchedulingInfo,
                           host, port, exSystem.actorSelection(stageSchedulingInfo.topologyExecutionManagerActorUrl))), name = executorId)
  }

  def buildExecutorWorkerActor(context:ActorContext, workerId:Int, executorId: String, executionContext: StageExecutionContext): ActorRef = {
    logInfo(s"buildExecutorWorkerActor: Creating executor worker actor with executorId:$executorId and workerId:$workerId")
    context.actorOf(Props(new ExecutorWorker(workerId, executorId, executionContext)))
  }

  def resolveActor(actorSelection: ActorSelection): ActorRef = {
    Await.result[ActorRef](actorSelection.resolveOne()(akka.util.Timeout.intToTimeout(1)), 1 seconds)
  }

  def resolveActor(system:ActorSystem, actorPath:String ): ActorRef = {
    this.resolveActor(system.actorSelection(actorPath))
  }
}
