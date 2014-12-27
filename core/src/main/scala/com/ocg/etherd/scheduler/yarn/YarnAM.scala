package com.ocg.etherd.scheduler.yarn

import java.util
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.util.ConverterUtils
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.api.protocolrecords._
import org.apache.hadoop.yarn.client.api._
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.client.api.AMRMClient
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest

import scala.collection.mutable._

class YarnAM(yarnConf: YarnConfiguration, queue: String) {
  private var amClient: AMRMClient[ContainerRequest] = _
  private var nmClient: NMClient = _
  private var registerResponse: RegisterApplicationMasterResponse = _

  def init() {

    // set up user group information, setup a new yarn client and create a default application
    UserGroupInformation.setConfiguration(yarnConf)
    val ugi = UserGroupInformation.getCurrentUser
    val yarnClient = startYarnClient(yarnConf)
    val appId = createApplication(yarnClient, this.queue)
    ugi.addToken(yarnClient.getAMRMToken(appId))

    // create clients to talk to Resource Manager and Node Manager
    amClient = AMRMClient.createAMRMClient()
    amClient.init(yarnConf)
    amClient.start()

    nmClient = NMClient.createNMClient()
    nmClient.init(yarnConf)
    nmClient.start()

    // Register this application Master
    registerResponse = amClient.registerApplicationMaster("localhost", 0, "")
  }

  def close(){
      this.amClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "success", "")
  }

  def allocate(command: String){
    val allocator = new Allocater(yarnConf, amClient, nmClient)
    allocator.beginAllocate (amClient.getAvailableResources, registerResponse.getMaximumResourceCapability,
      this.createResourceRequests(1, 1024),
      () => this.setUpContainerLaunchContext(command),
      (statuses: java.util.List[ContainerStatus]) => {/* Called on completion. we will print some status here*/})
  }

  def setUpContainerLaunchContext(command: String) : ContainerLaunchContext = {
    val lc = ContainerLaunchContext.newInstance(null, null, null, null,null, null)
    val commands = new util.LinkedList[String]()
    commands.add(command)
    lc.setCommands(commands)
    lc
  }

  def startYarnClient(yarnConf: YarnConfiguration) : YarnClient = {
    val yarnClient = YarnClient.createYarnClient
    yarnClient.init(yarnConf)
    yarnClient.start()
    yarnClient
  }

  def createApplication(rmClient: YarnClient, queue: String) : ApplicationId= {
    val newApp = rmClient.createApplication()
    val appId = newApp.getNewApplicationResponse.getApplicationId

    val pri = Priority.newInstance(0)
    val amContainer = ContainerLaunchContext.newInstance(null, null, null, null, null, null)
    val appContext = ApplicationSubmissionContext.newInstance(appId, "Etherd topology queue for " + queue,
                                                              queue, pri, amContainer,
                                                              true, false, 1, null, "Etherd topology queue")
    // Submit the application to the applications manager
    rmClient.submitApplication(appContext)
  }

  def createResourceRequests(workers: Int, memory: Int) : ListBuffer[ContainerRequest] = {
    val reqs = new ListBuffer[ContainerRequest]
    val resource = Resource.newInstance(memory, workers)
    reqs += new ContainerRequest(resource, null, null, Priority.newInstance(1), true)
    reqs
  }

  def getNextAttemptid : ApplicationAttemptId = {
    val t = java.lang.System.currentTimeMillis()
    ConverterUtils.toApplicationAttemptId(ConverterUtils.APPLICATION_ATTEMPT_PREFIX + "_" + t.toString() + "_0001" + "_0001")
  }
}

package object YarnAM extends App {
  val conf = new Configuration()
  conf.addResource(new Path("/Users/loaner/hadoop-2.2.0/etc/hadoop/hdfs-site.xml"))
  conf.addResource(new Path("/Users/loaner/hadoop-2.2.0/etc/hadoop/core-site.xml"))
  conf.addResource(new Path("/Users/loaner/hadoop-2.2.0/etc/hadoop/yarn-site.xml"))
  conf.reloadConfiguration()
  val yarnConf = new YarnConfiguration(conf)
  yarnConf.addResource("/Users/loaner/hadoop-2.2.0/etc/hadoop/yarn-site.xml")
  println("Yarn is listening on " + yarnConf.get("yarn.resourcemanager.scheduler.address"))

  val am = new YarnAM(yarnConf, "default")
  try{
    am.init()
    am.allocate("touch /tmp/yarntest")
  }
  finally{
    am.close()
  }
}