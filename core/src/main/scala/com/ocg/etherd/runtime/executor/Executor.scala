package com.ocg.etherd.runtime.executor

import java.util.concurrent.atomic.AtomicInteger
import com.ocg.etherd.runtime.StageExecutionContext

import scala.concurrent.duration._
import akka.actor._
import akka.actor.SupervisorStrategy._
import com.ocg.etherd.Logging
import com.ocg.etherd.runtime.RuntimeMessages._
import com.ocg.etherd.runtime.akkautils._
import com.ocg.etherd.runtime.scheduler.SchedulableTask
import com.ocg.etherd.topology.{Stage, StageSchedulingInfo}

import scala.util.Random

/**
 * Supervisor Actor whose instance is created and started by the scheduler on the worker node.
 * This actor does not run the stage itself but starts a new child actor to run a stage
 * @param executorId
 * @param stageSchedulingInfo
 * @param host
 * @param port
 * @param topologyExecutionManagerActor
 */
private[etherd] class Executor(executorId: String,
                               stageSchedulingInfo:StageSchedulingInfo,
                               host:String,
                               port: Int,
                               topologyExecutionManagerActor: ActorSelection) extends Actor with Logging {
  val workerIdInc = new AtomicInteger(1)
  val executorActorUrl = self.path.toStringWithAddress(RemoteAddressExtension(context.system).address)
  // child actors
  var stageExecutionActor: Option[ActorRef] = None // child actor that will run the stage

  /**
   * For now for all exceptions we will just resume and delegate the rest to the parent
   * ?? this needs to change after putting some thought here.
   */
  override val supervisorStrategy = OneForOneStrategy() {
    case _: ActorInitializationException => {
      logWarning(s"ActorInitializationException for executor $executorActorUrl")
      // failed to create the actor. Notify execution manager that we could not start the stage
      Stop
    }
    case _: ActorKilledException => {
      logWarning(s"ActorKilledException for executor $executorActorUrl")
      //we should just ignore here unless we did not know apriori about it
      Stop
    }
    case ex: Exception => {
      logWarning("Executor supervisor strategy restarting the child actor:" + ex.toString)
      // ?? restrict number of retries
      Restart
    }
  }

  /**
   * Akka calls this the first time when the actor starts and also after a restart
   * 1. Load state from checkpoint if any if there was a restart
   * 2. Send a registration back to the execution manager
   * 3. Wait for ExecuteStage message
   */
  override def preStart() = {
    // ?? create the event manager actor
    // ?? create log manager actor
    // ?? create state manager actor
    logDebug(s"Starting executor actor: $executorId. Sending registration to execution manager")
    val executorData = ExecutorData(executorId, stageSchedulingInfo.stageId, stageSchedulingInfo.partition, host, port, Some(this.executorActorUrl))
    this.topologyExecutionManagerActor ! RegisterExecutor(stageSchedulingInfo.topologyId, executorData)
  }

  /**
   * This is called when akka restarts the actor.
   * 1. Checkpoint state so we can correctly restart
   * @param reason
   * @param message
   */
  override def preRestart(reason: Throwable, message: Option[Any]) = {
    val reasonStr = reason.toString
    logInfo(s"ExecutorId: $executorId restarted because $reasonStr")
    super.preRestart(reason, message)
  }

  override def postStop() = {
    logDebug(s"ExecutorId: $executorId stopping")
  }

  def receive = {
    case ExecuteStage(stage: Stage) => synchronized {
      this.stageExecutionActor match {
        case Some(actor) => {
          logError(s"Worker Actor already started for executor $executorId")
        }
        case None => {
          this.tryRunStage(stage)
        }
      }
    }

    case ControlledShutdown(shutdownReason: String) => {
      // ask system to shutdown. This should trigger shutdown hooks
      // and cleanup all child actors
      logInfo(s"Executor $executorId. Received ControlledShutdown. Reason: $shutdownReason")
      this.context.system.shutdown()
    }

    case _ => logError("Unknown message received")
  }

  private def tryRunStage(stage: Stage): Unit = {
    val stageId = stage.stageId.get
    logDebug(s"Executor: $executorId. Starting worker actor")
    assert(stageId == this.stageSchedulingInfo.stageId, "We should get the same stageId that we registered with")

    // create a worker actor and delegate the stage execution to it
    val executorWorker = Utils.buildExecutorWorkerActor(context,
      this.workerIdInc.getAndIncrement,
      this.executorId,
      StageExecutionContext(stage, this.stageSchedulingInfo.partition)
    )
    executorWorker ! RunStage
    this.stageExecutionActor = Some(executorWorker)
  }
}

object Executor {
  def startNew(schedulable: SchedulableTask[_]): ActorSystem = synchronized  {
    val stageScheduleInfo = schedulable.asInstanceOf[SchedulableTask[StageSchedulingInfo]].getTaskInfo
    val topologyExecutionManagerActorUrl = stageScheduleInfo.topologyExecutionManagerActorUrl
    val topologyName = stageScheduleInfo.topologyId
    val stageId = stageScheduleInfo.stageId
    val partition = stageScheduleInfo.partition
    val randomPort = 9000 + Random.nextInt(500)
    val hostname = java.net.InetAddress.getLocalHost.getCanonicalHostName

    val nhostname = hostname.replace('.', '-' )
    val executorId = s"$topologyName-$stageId-$partition-$nhostname-$randomPort"

    val actorSystem = Utils.buildActorSystem(s"executorSystem-$executorId", randomPort)
    Utils.buildExecutorActor(actorSystem, executorId, stageScheduleInfo, hostname, randomPort)
    actorSystem
  }
}
