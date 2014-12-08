package com.ocg.etherd.testbase

import org.scalatest._
import com.ocg.etherd.streams._
import com.ocg.etherd.spn.SPN
import com.ocg.etherd.topology.SPNExecutionContext

/**
 *
 */
abstract class UnitSpec extends FlatSpec with Matchers with
OptionValues with Inside with Inspectors
{
  def simple(istream: ReadableEventStream, ostream: WriteableEventStream): SPNExecutionContext = {
    new SPNExecutionContext(Set(istream), (s) => ostream, 1)
  }

  def simple(istreams: Set[ReadableEventStream], ostream: WriteableEventStream): SPNExecutionContext = {
    new SPNExecutionContext(istreams, (s) => ostream, 1)
  }


}
