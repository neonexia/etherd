package com.ocg.etherd.runtime

import akka.actor.{ActorRef, ActorSystem, Actor, ActorSelection}
import akka.event.Logging
import com.ocg.etherd.ActorUtils
import com.ocg.etherd.runtime.scheduler.SchedulableTask
import com.ocg.etherd.topology.{StageSchedulingInfo, Stage}
import RuntimeMessages._

import scala.util.Random

class Executor(executorId: String, stageSchedulingInfo:StageSchedulingInfo,
               host:String, port: Int, topologyExecutionManagerActor: ActorSelection) extends Actor  {
  val log = Logging(context.system, executorId)
  var runtimeState = RuntimeState.Init

  override def preStart() = {
    this.runtimeState = RuntimeState.Init
    log.info(s"ExecutorId: $executorId. Sending registration to execution manager")
    this.topologyExecutionManagerActor ! RegisterExecutor(stageSchedulingInfo.topologyId, ExecutorData(executorId, stageSchedulingInfo.stageId, host, port))
  }

  def receive = {

    case ExecuteStage(stage: Stage) => synchronized {
      assert(this.runtimeState != RuntimeState.Scheduled)
      this.runtimeState = RuntimeState.Scheduled

      val stageId = stage.stageId.get
      log.info(s"Executor: $executorId. Received ExecuteStage with stageId $stageId")
      assert(stageId==this.stageSchedulingInfo.stageId, "We should get the same stageId that we registered with")
      StageExecutionContext(stage).run()
    }

    case _ => log.error("Unknown message received")
  }
}

object Executor {

  def startNew(schedulable: SchedulableTask[_]): ActorSystem = synchronized  {
    val stageScheduleInfo = schedulable.asInstanceOf[SchedulableTask[StageSchedulingInfo]].getTaskInfo
    val topologyExecutionManagerActorUrl = stageScheduleInfo.topologyExecutionManagerActorUrl
    val topologyName = stageScheduleInfo.topologyId
    val stageId = stageScheduleInfo.stageId
    val randomPort = 9000 + Random.nextInt(500)
    val hostname = java.net.InetAddress.getLocalHost.getCanonicalHostName

    val hostnameCleanedup = hostname.replace('.', '-' )
    val executorId = s"$topologyName-$stageId-$hostnameCleanedup-$randomPort"

    val actorSystem = ActorUtils.buildActorSystem(s"executorSystem-$executorId", randomPort)

    println(s"Execution Manager Url: $topologyExecutionManagerActorUrl")
    ActorUtils.buildExecutorActor(actorSystem, executorId, stageScheduleInfo, hostname, randomPort)
    actorSystem
  }
}




