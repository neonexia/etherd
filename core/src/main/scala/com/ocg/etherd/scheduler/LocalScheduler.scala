package com.ocg.etherd.scheduler

import java.util.concurrent.{Executors, ExecutorService}

/**
 *
 */
class LocalScheduler(maxCores: Int) extends Scheduler {

  val pool: ExecutorService = Executors.newFixedThreadPool(maxCores)

  def reviveOffers(): Unit = {
    
  }
}
