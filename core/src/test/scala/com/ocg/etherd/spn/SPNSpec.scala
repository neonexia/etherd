package com.ocg.etherd.spn

import com.ocg.etherd.testbase.UnitSpec
import com.ocg.etherd.streams._
import com.ocg.etherd.topology.Stage
import scala.collection.mutable

/**
 */
class SPNSpec extends UnitSpec {
  "A Passthrough SPN" should "build a single stage" in {
    val spn = buildPass
    var finalStageList = mutable.ListBuffer.empty[Stage]
    spn.buildStages(finalStageList)
    assertResult(1) {
      finalStageList.size
    }
  }

  it should "if combined with MappedSPN build a single stage" in{
    val spn = buildPass
    val map = spn.map {ev => ev}
    var finalStageList = mutable.ListBuffer.empty[Stage]
    map.buildStages(finalStageList)
    assertResult(1) {
      finalStageList.size
    }
  }

  it should "when sink into 2 SPN's build 3 stages" in {
    val spn = buildPass
    val spn1 = buildPass
    val spn2 = buildPass
    spn.sink(List[SPN](spn1, spn2))
    var finalStageList = mutable.ListBuffer.empty[Stage]
    spn.buildStages(finalStageList)
    assertResult(3) {
      finalStageList.size
    }
  }

  def buildPass: SPN ={
    val q = mutable.Queue[Event]()
    val istream = EventStream.sampleRange("default", 10)
    val wstream = EventStream.sampleWritabletream(q)
    val spn = SPN.pass(this.simple(istream, wstream))
    spn
  }
}
