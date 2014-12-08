package com.ocg.etherd.topology

import com.ocg.etherd.streams.{WriteableEventStream, ReadableEventStream}
import com.ocg.etherd.spn.SPN

class SPNExecutionContext(istreams: Set[ReadableEventStream],
                          newOutStream: (String) => WriteableEventStream, partitionId: Int) {

  def getInputStreams : Set[ReadableEventStream] = this.istreams

  def defaultOutStream: WriteableEventStream = this.newOutStream("default")

  def getNewStage(spn: SPN) = new Stage(spn)

  def waitForCompletion() = {
    this.wait()
  }

  def signalComplete() = {
    this.notifyAll()
  }
}