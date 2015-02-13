package com.ocg.etherd.runtime

import akka.actor.{ActorRef, ActorSystem, Actor, ActorSelection}
import akka.event.Logging
import com.ocg.etherd.ActorUtils
import com.ocg.etherd.runtime.scheduler.SchedulableTask
import com.ocg.etherd.topology.Stage
import RuntimeMessages._

import scala.util.Random

class Executor(executorId: String, host:String, port: Int,
               topologyName:String, topologyExecutionManagerActor: ActorSelection) extends Actor  {
  val log = Logging(context.system, this)
  println(s"ExecutorId: $executorId. Sending registration to execution manager")
  this.topologyExecutionManagerActor ! RegisterExecutor(topologyName, ExecutorData(executorId, 0, host, port))

  def receive = {
    case RuntimeMessages.ExecuteStage(topologyName: String, stage: Stage) => {
      log.info("Received ExecuteStage message")
    }
    case _ => log.info("")
  }
}

object Executor {
  var actorSystem: Option[ActorSystem] = None

  def apply(executorId: String, host:String, port: Int,
            topologyName:String, topologyExecutionManagerActor: ActorSelection): Executor = {
    new Executor(executorId, host, port, topologyName, topologyExecutionManagerActor)
  }

  def start(schedulable: SchedulableTask[_]): ActorRef = synchronized  {
    val stage = schedulable.asInstanceOf[SchedulableTask[Stage]].getTaskInfo
    val randomPort = 9000 + Random.nextInt(500)
    val hostname = java.net.InetAddress.getLocalHost.getCanonicalHostName
    val stageId = stage.getStageId.get
    val topologyName = stage.getTopologyId.get
    val hostnameCleanedup = hostname.replace('.', '-' )
    val executorId = s"$topologyName-$stageId-$hostnameCleanedup-$randomPort"

    actorSystem = Some(ActorUtils.buildActorSystem(s"executorSystem-$executorId", randomPort))
    val topologyExecutionManagerActorUrl = stage.getTopologyExecutionManagerActorUrl.get
    println(s"Execution Manager Url: $topologyExecutionManagerActorUrl")
    ActorUtils.buildExecutorActor(executorId, hostname, randomPort, actorSystem.get, topologyName, topologyExecutionManagerActorUrl, executorId)
  }

  def shutdown(): Unit = {
    actorSystem.map { system =>
      system.shutdown()
    }
  }
}




