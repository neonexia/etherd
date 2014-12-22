package com.ocg.etherd.messaging

/**
 * Abstracts out a distributed messaging queue for isolation between multiple stages
 * of a topology or for composing topologies
 * - topics- High level event categories
 * - topic partitions (distributed)
 * - per topic publish async streams (Single default stream)
 * - per topic partition subscription async. streams (multiple streams with a starting offset)
 * - intermediate topics for topology executions.
 * - logging and monitoring listener subscriptions
 */

trait  DMessageQueue {
  def getEventQueue(topic: String): DMessageQueueStream

  def buildFrom(ostream: DMessageQueueStream) : DMessageQueueStream
}



