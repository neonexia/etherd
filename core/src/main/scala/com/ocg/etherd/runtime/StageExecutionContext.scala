package com.ocg.etherd.runtime

import com.ocg.etherd.topology.Stage

class StageExecutionContext(stage: Stage) {
  var runtimeState = RuntimeState.Init

  def run(): Unit = {
    assert(this.runtimeState != RuntimeState.Scheduled)
    this.runtimeState = RuntimeState.Scheduled
    println("StageExecutionContext: beginProcessStreams")
    this.stage.underlying.beginProcessStreams()
  }

  def stop(): Unit = {
     // stop the streams
  }
}

object StageExecutionContext {
  def apply(stage: Stage): StageExecutionContext = {
    new StageExecutionContext(stage)
  }
}
