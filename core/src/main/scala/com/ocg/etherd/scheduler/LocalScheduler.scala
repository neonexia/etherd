package com.ocg.etherd.scheduler

import java.util.concurrent.{Executors, ExecutorService}

/**
 *
 */
class LocalScheduler(maxCores: Int, memFraction: Int) extends Scheduler {

  def numMaxCores = {
    if (maxCores == 0)
      Runtime.getRuntime.availableProcessors
    else
      maxCores
  }

  def numMemFraction = {
    val th = Runtime.getRuntime.maxMemory * 60/100
    if (memFraction == 0)
      th
    else
      Math.max(memFraction, th)
  }
  val pool: ExecutorService = Executors.newFixedThreadPool(numMaxCores)

  def this() = this(0,0)

  def reviveOffers(): Unit = {

  }
}
