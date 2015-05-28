package com.ocg.etherd.runtime.scheduler

import java.util.concurrent.{Executors, ExecutorService}
import com.ocg.etherd.runtime.executor.Executor
import com.ocg.etherd.util.HostUtil
import scala.collection.mutable
import akka.actor.ActorSystem

/**
 * Local scheduler that runs tasks on local thread pool.
 * Sets available processors to some high number and 60% of max available JVM memory(-Xmx)
 */
private[etherd] class LocalScheduler(maxCores: Int, memoryFraction: Int) extends Scheduler {
  val hostResource = ClusterResource(usableCores, maxMemory, "localhost")
  val pool: ExecutorService = Executors.newFixedThreadPool(usableCores)
  val executorList =  mutable.ListBuffer.empty[ActorSystem]

  def this() = this(0, 60)

  def usableCores: Int = {
    if (maxCores == 0)
      // some high number
      100
    else
      Math.min(maxCores, HostUtil.availableCores)
  }

  def maxMemory: Int = {
    HostUtil.maxMemoryG(memoryFraction)
  }

  def reviveOffers(): Unit = {
    val schedulableTasks = this.getCandidateTasks(hostResource)
    schedulableTasks.foreach { schedulable => {
        try {
          logDebug("Local thread pool...starting executor")
          val executor = Executor.startNew(schedulable)
          executorList += executor
        }
        catch {
          case e: Exception => logError(s"Exception when starting executor: $e")
        }
      }
    }
  }

  def shutdownTasks(): Unit = {
    this.executorList.foreach(executor => executor.shutdown())
  }
}
