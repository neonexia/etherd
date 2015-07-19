package com.ocg.etherd.runtime.scheduler

import java.util.concurrent.{Executors, ExecutorService}
import scala.collection.mutable
import akka.actor.{Actor, ActorSystem}

import com.ocg.etherd.runtime.RuntimeMessages._
import com.ocg.etherd.runtime.executor.Executor
import com.ocg.etherd.util.HostUtil

/**
 * Local scheduler that just starts the executor actor systems.
 */
private[etherd] class LocalScheduler(maxCores: Int, memoryFraction: Int) extends Actor with Scheduler {
  val hostResource = ClusterResource(usableCores, maxMemoryG, "localhost")
  val executorList =  mutable.ListBuffer.empty[ActorSystem]

  def this() = this(0, 0)

  // delegate to baseReceive if we cannot handle the message
  override def receive = localSchedulerReceive orElse baseSchedulerReceive
  def localSchedulerReceive: Receive = {
	case ShutdownAllScheduledTasks() => {
		this.shutdownTasks()
	}
  }

  def usableCores: Int = {
    if (maxCores == 0)
      // some high number
      100
    else
      Math.min(maxCores, HostUtil.availableCores)
  }

  def maxMemoryG: Int = {
    if (memoryFraction == 0)
      //some high number
      100
	else
      HostUtil.maxMemoryG(memoryFraction)
  }

  def reviveOffers(): Unit = {
    this.getCandidateTasks(hostResource).foreach { schedulableTask => {
	  logDebug("Local Scheduler...starting executor actor system")
	  val executor = Executor.startNew(schedulableTask)
	  executorList += executor
      }
    }
  }

  def shutdownTasks(): Unit = {
    this.executorList.foreach(executor => executor.shutdown())
  }


}
