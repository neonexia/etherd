package com.ocg.etherd.runtime.scheduler

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ListBuffer

import com.ocg.etherd.Logging

/**
 * Scheduler base class for Resource manager specific concrete classes
 */
private[etherd] abstract class Scheduler extends Logging {
  val submittedTaskQueue = new ConcurrentLinkedQueue[SchedulableTask[_]]()
  type ReviveOffersType = () => Unit

  def getPendingTasks = this.submittedTaskQueue

  def submit(tasks: Iterator[SchedulableTask[_]]): Unit = {
    logInfo("Received tasks for scheduling")
    tasks.foreach { task => this.submittedTaskQueue.offer(task)}
    this.reviveOffers()
  }

  protected def getCandidateTasks(offeredResource: ClusterResource): ListBuffer[SchedulableTask[_]] = {
    var schedulableTasks = new ListBuffer[SchedulableTask[_]]()
    val it = this.submittedTaskQueue.iterator

    while (it.hasNext && !offeredResource.empty) {
      val task = it.next
      if (task.canSchedule(offeredResource)) {
        schedulableTasks += task
        offeredResource.consumeCores(task.getResourceAsk.getCores)
        offeredResource.consumeMemory(task.getResourceAsk.getMemory)
      }
    }

    // if we have a matching tasks deque from the submitted Queue
    schedulableTasks.foreach { task =>
      this.submittedTaskQueue.remove(task)
    }

    schedulableTasks
  }

  def reviveOffers(): Unit

  def shutdownTasks(): Unit
}

class ResourceAsk(cores: Int, memory: Int) {

  def getCores = cores

  def getMemory = memory
}

class ClusterResource(host: String) {

  var availableCores: AtomicInteger = new AtomicInteger(0)
  var availableMemory = new AtomicInteger(0)

  def getCores = this.availableCores.get

  def getMemory = this.availableMemory.get

  def offerCores(cores: Int) = this.availableCores.addAndGet(cores)

  def offerMemory(memory: Int) = this.availableMemory.addAndGet(memory)

  def consumeCores(cores: Int) = this.availableCores.addAndGet(cores * -1)

  def consumeMemory(memory: Int) = this.availableMemory.addAndGet(memory * -1)

  def empty = {
    this.availableCores.get <= 0 || this.availableMemory.get <= 0
  }
}

class SchedulableTask[T](taskInfo: T, resourceAsk: ResourceAsk) {

  def getResourceAsk = this.resourceAsk

  def getTaskInfo = this.taskInfo

  def canSchedule(offeredResource: ClusterResource): Boolean = {
    this.resourceAsk.getCores <= offeredResource.getCores &&
    this.resourceAsk.getMemory <= offeredResource.getMemory
  }
}


object ClusterResource {
  def apply(cores: Int, memory: Int, host: String): ClusterResource = {
    val r = new ClusterResource(host)
    r.offerCores(cores)
    r.offerMemory(memory)
    r
  }
}

