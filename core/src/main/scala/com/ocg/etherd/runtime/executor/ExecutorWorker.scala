package com.ocg.etherd.runtime.executor

import akka.actor.Actor
import com.ocg.etherd.Logging
import com.ocg.etherd.runtime.RuntimeMessages.RunStage
import com.ocg.etherd.runtime.StageExecutionContext

/**
 * Executor Worker actor that will be started by the Executor Supervisor actor.
 * On start will begin stage execution
 */
private[etherd] class ExecutorWorker(workerId:Int, executorId: String, executionContext: StageExecutionContext) extends Actor with Logging {
  val workerName = s"ExecutorWorker-$workerId-$executorId"

  def receive = {
    case RunStage => this.executeStage()
    case _ => logError("Unknown message received")
  }

  private def executeStage() = {
    this.executionContext.run()
  }
}
