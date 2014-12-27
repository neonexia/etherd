package com.ocg.etherd.scheduler.yarn

import scala.collection.mutable._

package object Utils {
  def getScalaList[T](jlist: java.util.List[T]) : ListBuffer[T] = {
    val l: ListBuffer[T] = new ListBuffer[T]
    val iter = jlist.iterator()
    while(iter.hasNext){
      l += iter.next()
    }
    l
  }
}
