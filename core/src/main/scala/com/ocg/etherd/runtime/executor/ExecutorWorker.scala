package com.ocg.etherd.runtime.executor

import akka.actor.Actor
import akka.event.Logging
import com.ocg.etherd.runtime.StageExecutionContext

/**
 * Executor Worrker actor that will be started by the Executor Supervisor actor.
 * On start will start executing the stage
 */
private[etherd] class ExecutorWorker(workerId:Int, executorId: String, executionContext: StageExecutionContext) extends Actor {
  val workerName = s"ExecutorWorker-$workerId-$executorId"
  val log = Logging(context.system, this.workerName)
  this.executeStage()

  def receive = {
    case _ => log.error("Unknown message received")
  }

  private def executeStage() = {
    log.info("Running StageExecutionContext")
    this.executionContext.run()
  }
}
