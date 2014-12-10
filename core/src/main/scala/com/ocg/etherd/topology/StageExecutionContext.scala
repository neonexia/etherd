package com.ocg.etherd.topology

import com.ocg.etherd.spn.SPN

class StageExecutionContext() {

  def getNewStage(spn: SPN) = new Stage(spn)

  def waitForCompletion() = {
    this.wait()
  }

  def signalComplete() = {
    this.notifyAll()
  }
}