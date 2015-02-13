package com.ocg.etherd.runtime.scheduler.yarn

import scala.collection.mutable.ListBuffer
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api._
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.client.api.AMRMClient
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest

class Allocater(yarnConf: YarnConfiguration, amClient: AMRMClient[ContainerRequest], nmClient: NMClient) {

  def beginAllocate(availableResources: org.apache.hadoop.yarn.api.records.Resource,
                    maxResources: org.apache.hadoop.yarn.api.records.Resource,
                    requests: ListBuffer[ContainerRequest],
                    onAllocation: () => ContainerLaunchContext,
                    onContainerComplete: (java.util.List[ContainerStatus]) => Unit){

    for (req <- requests){
      amClient.addContainerRequest(req)
    }

    var allocSize: Int = 0
    var tries: Int = 0
    var done: Boolean = false
    while(!done)
    {
      val allocateResponse = amClient.allocate(0)
      val allocatedContainers = allocateResponse.getAllocatedContainers
      for(container <- Utils.getScalaList(allocatedContainers)) {
        allocSize += 1
        val lc = onAllocation()
        nmClient.startContainer(container, lc)
      }
      if (allocSize < 1 && tries < 5)
      {
        Thread.sleep(100)
        tries += 1
      }
      else
        done = true
    }

    if(allocSize > 0)
    {
      var completedContainers = 0
      var done: Boolean = false
      while(!done){
        val allocateResponse = amClient.allocate(0)
        val statuses = allocateResponse.getCompletedContainersStatuses
        for (status <- Utils.getScalaList(statuses)){
          completedContainers += 1
        }
        onContainerComplete(statuses)
        if (completedContainers == allocSize)
          done = true
        else
          Thread.sleep(100)
      }
    }
  }
}
