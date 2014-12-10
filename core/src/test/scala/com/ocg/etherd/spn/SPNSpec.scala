package com.ocg.etherd.spn

import com.ocg.etherd.testbase.UnitSpec
import com.ocg.etherd.streams._
import com.ocg.etherd.topology.Stage
import scala.collection.mutable

/**
 */
class SPNSpec extends UnitSpec {
  "A Passthrough SPN" should "build a single stage" in {
    val spn = ingest()
    var finalStageList = mutable.ListBuffer.empty[Stage]
    spn.buildStages(finalStageList)
    assertResult(1) {
      finalStageList.size
    }
  }

  it should "if combined with MappedSPN build a single stage" in{
    val spn = ingest()
    val map = spn.map {ev => ev}
    var finalStageList = mutable.ListBuffer.empty[Stage]
    map.buildStages(finalStageList)
    assertResult(1) {
      finalStageList.size
    }
  }

  it should "when sink into 2 SPN's build 3 stages" in {
    val spn = ingest()
    val spn1 = ingest()
    val spn2 = ingest()
    spn.sink(List[SPN](spn1, spn2))
    var finalStageList = mutable.ListBuffer.empty[Stage]
    spn.buildStages(finalStageList)
    assertResult(3) {
      finalStageList.size
    }
  }
}
