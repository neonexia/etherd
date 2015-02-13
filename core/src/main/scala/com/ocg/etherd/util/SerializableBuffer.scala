package com.ocg.etherd.util


import java.io.{EOFException, IOException, ObjectInputStream, ObjectOutputStream}
import java.nio.ByteBuffer
import java.nio.channels.Channels

/**
 * A wrapper around a java.nio.ByteBuffer that is serializable through Java serialization, to make
 * it easier to pass ByteBuffers in case class messages.
 *
 * Code ported from Spark ($repo_home/spark/core/src/main/scala/org/apache/spark/util/SerializableBuffer.scala)
 *
 */
private[etherd]
class SerializableBuffer(@transient var buffer: ByteBuffer) extends Serializable {
  def value = buffer

  private def readObject(in: ObjectInputStream): Unit =  {
    val length = in.readInt()
    buffer = ByteBuffer.allocate(length)
    var amountRead = 0
    val channel = Channels.newChannel(in)
    while (amountRead < length) {
      val ret = channel.read(buffer)
      if (ret == -1) {
        throw new EOFException("End of file before fully reading buffer")
      }
      amountRead += ret
    }
    buffer.rewind() // Allow us to read it later
  }

  private def writeObject(out: ObjectOutputStream): Unit = {
    out.writeInt(buffer.limit())
    if (Channels.newChannel(out).write(buffer) != buffer.limit()) {
      throw new IOException("Could not fully write buffer to output stream")
    }
    buffer.rewind() // Allow us to write it again later
  }
}
