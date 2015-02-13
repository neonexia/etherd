package com.ocg.etherd

import com.ocg.etherd.messaging.LocalDMessageBus
import com.ocg.etherd.runtime.scheduler.LocalScheduler

class EtherdEnv() {
  val scheduler = this.getScheduler
  val messageBus = this.getMessageBus

  def getScheduler = new LocalScheduler(2, 2)

  def getMessageBus = new LocalDMessageBus()
}

object EtherdEnv {
  var env = new EtherdEnv()

  def get = {
    env
  }
}
