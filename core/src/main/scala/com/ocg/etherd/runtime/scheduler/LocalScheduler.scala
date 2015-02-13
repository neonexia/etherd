package com.ocg.etherd.runtime.scheduler

import java.util.concurrent.{Executors, ExecutorService}
import com.ocg.etherd.runtime.Executor
import com.ocg.etherd.topology.Stage

/**
 *
 */
class LocalScheduler(cores: Int, memoryFraction: Int) extends Scheduler {
  val hostResource = ClusterResource(maxCores, maxMemory, "localhost")
  val pool: ExecutorService = Executors.newFixedThreadPool(maxCores)

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
    val schedulableTasks = this.consumeOfferedResources(hostResource)
    schedulableTasks.foreach { schedulable => {
        try {
          println("Thread pool...starting executor")
          Executor.start(schedulable)
          println("Executor started. Thread ending")
        }
        catch {
          case e: Exception => println(s"Exception when starting executor: $e")
        }
      }
    }
  }
}
