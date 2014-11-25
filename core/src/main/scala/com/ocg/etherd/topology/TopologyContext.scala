package com.ocg.etherd.topology

import com.ocg.etherd.streams.{WriteableEventStream, ReadableEventStream}


class TopologyContext(sc: StageContext,
                      istreams: Set[ReadableEventStream],
                      newOutStream: (String) => WriteableEventStream,
                      partitionId: Int) {

  def getInputStreams : Set[ReadableEventStream] = this.istreams

  def defaultOutStream: WriteableEventStream = this.newOutStream("default")

  def waitForCompletion() = {
    this.wait()
  }

  def signalComplete() = {
    this.notifyAll()
  }
}

object TopologyContext {

}