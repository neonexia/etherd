package com.ocg.etherd.runtime.scheduler

import java.util.concurrent.{Executors, ExecutorService}
import com.ocg.etherd.runtime.executor.Executor

import scala.collection.mutable
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import akka.actor.ActorSystem
import com.ocg.etherd.topology.Stage

/**
 *
 */
private[etherd] class LocalScheduler(cores: Int, memoryFraction: Int) extends Scheduler {
  val hostResource = ClusterResource(maxCores, maxMemory, "localhost")
  val pool: ExecutorService = Executors.newFixedThreadPool(maxCores)
  val executorList =  mutable.ListBuffer.empty[ActorSystem]

  def this() = this(0, 60)

  def maxCores = {
    if (cores == 0)
      Runtime.getRuntime.availableProcessors
    else
      Math.min(cores, Runtime.getRuntime.availableProcessors)
  }

  def maxMemory = {
    if (memoryFraction == 0)
      (Runtime.getRuntime.maxMemory() / 1000).asInstanceOf[Int]
    else
      (Runtime.getRuntime.maxMemory() / 1000 * memoryFraction / 100).asInstanceOf[Int]
  }

  def reviveOffers(): Unit = {
    println("Revive offers")
    val schedulableTasks = this.getCandidateTasks(hostResource)
    schedulableTasks.foreach { schedulable => {
        try {
          println("Local thread pool...starting executor")
          val executor = Executor.startNew(schedulable)
          executorList += executor
        }
        catch {
          case e: Exception => println(s"Exception when starting executor: $e")
        }
      }
    }
  }

  def shutdownTasks(): Unit = {
    this.executorList.foreach(executor => executor.shutdown())
  }
}
