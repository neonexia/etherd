package com.ocg.etherd.util

/**
 * Utility class to get information about the current host
 */
object HostUtil {
  def availableCores = {
    Runtime.getRuntime.availableProcessors
  }

  def maxMemoryG(memoryFraction: Int = 0) = {
    if (memoryFraction == 0)
      (Runtime.getRuntime.maxMemory() / 1000).asInstanceOf[Int]
    else
		((Runtime.getRuntime.maxMemory() / 1000) * (memoryFraction / 100f)).asInstanceOf[Int]
  }
}
