package com.ocg.etherd.runtime.scheduler

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ListBuffer
import akka.actor.Actor

import com.ocg.etherd.Logging
import com.ocg.etherd.runtime.RuntimeMessages.ScheduleTasks

import scala.reflect.ClassTag

/**
 * Scheduler actor base class
 */
private[etherd] trait Scheduler extends Actor with Logging {

  val submittedTaskQueue = new ConcurrentLinkedQueue[SchedulableTask[_]]()

  // derived classes should delegate baseSchedulerReceive for unhandled message
  def baseSchedulerReceive: Receive = {
    case ScheduleTasks(tasks: Iterator[_]) => {
      tasks.foreach { task => this.submittedTaskQueue.offer(task)}
      //logInfo("Queuing tasks for scheduling. Queue size:" + this.submittedTaskQueue.size)
      this.reviveOffers()
    }
  }

  /**
   * Retrieves candidate tasks from the task queue that match the offered resource.
   * @param offeredResource
   * @return
   */
   def getCandidateTasks(offeredResource: ClusterResource): ListBuffer[SchedulableTask[_]] = {
    var schedulableTasks = new ListBuffer[SchedulableTask[_]]()
    val it = this.submittedTaskQueue.iterator

    while (it.hasNext && !offeredResource.empty) {
      val task = it.next
      if (task.canSchedule(offeredResource)) {
        schedulableTasks += task
        offeredResource.consumeCores(task.getResourceAsk.getCores)
        offeredResource.consumeMemory(task.getResourceAsk.getMemoryG)
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

class ResourceAsk(cores: Int, memoryG: Int) {

  def getCores = cores
  // memory in GB
  def getMemoryG = memoryG
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

object ClusterResource {
  def apply(cores: Int, memory: Int, host: String): ClusterResource = {
    val r = new ClusterResource(host)
    r.offerCores(cores)
    r.offerMemory(memory)
    r
  }
}

class SchedulableTask[T: ClassTag](taskInfo: T, resourceAsk: ResourceAsk) {

    def getResourceAsk = this.resourceAsk

    def getTaskInfo = this.taskInfo.asInstanceOf[T]

    def canSchedule(offeredResource: ClusterResource): Boolean = {
        this.resourceAsk.getCores <= offeredResource.getCores &&
                this.resourceAsk.getMemoryG <= offeredResource.getMemory
    }
}

