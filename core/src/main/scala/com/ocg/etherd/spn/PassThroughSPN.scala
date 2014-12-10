package com.ocg.etherd.spn
import com.ocg.etherd.streams._
import com.ocg.etherd.topology.StageExecutionContext

/**
 * Pass through
 * @param ec
 */
class PassThroughSPN(ec: StageExecutionContext, delay:Int = 0) extends SPN(ec) {

  override def processEvent(topic: String, event: Event): Unit = {
    if (delay > 0) {
      Thread.sleep(delay)
    }
    this.linkOrSinkDefault(topic, event)
   }
}
