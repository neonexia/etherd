package com.ocg.etherd.testbase

import org.scalatest._
import com.ocg.etherd.streams._
import com.ocg.etherd.spn.SPN
import com.ocg.etherd.topology.{StageContext, TopologyContext}

/**
 *
 */
abstract class UnitSpec extends FlatSpec with Matchers with
OptionValues with Inside with Inspectors
{
  def simple(istream: ReadableEventStream, ostream: WriteableEventStream): TopologyContext = {
    new TopologyContext(new StageContext, Set(istream), (s) => ostream, 1)
  }

  def simple(istreams: Set[ReadableEventStream], ostream: WriteableEventStream): TopologyContext = {
    new TopologyContext(new StageContext, istreams, (s) => ostream, 1)
  }
}
