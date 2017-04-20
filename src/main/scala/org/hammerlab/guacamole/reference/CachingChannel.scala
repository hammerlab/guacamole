package org.hammerlab.guacamole.reference

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel

import scala.collection.concurrent
import scala.math.{ max, min }

/**
 * Cache all reads from a [[SeekableByteChannel]] in blocks of size [[blockSize]].
 *
 * @param channel Underlying channel to buffer/cache bytes from.
 * @param blockSize Break the interval [0,channel.size) into blocks of this size, which are fetched+cached on demand.
 * @param maxReadAttempts Allow the underlying channel this many calls to `SeekableByteChannel.read` to fill
 *                        [[blockSize]] bytes of buffer.
 */
case class CachingChannel(channel: SeekableReadable,
                          blockSize: Int,
                          maxReadAttempts: Int = 2) {

  private var _buffer = ByteBuffer.allocate(blockSize)

  private var _position = 0L

  val blocks = concurrent.TrieMap[Long, ByteBuffer]()

//  def position(newPosition: Long): SeekableReadable = {
//    _position = newPosition
//    channel.seek(newPosition)
//  }

  def getBlock(idx: Long): ByteBuffer =
    blocks.getOrElseUpdate(
      idx,
      {
        _buffer.clear()
        val position = idx * blockSize
        channel.seek(position)
        var bytesRead = 0
        var attempts = 0
        while (bytesRead < _buffer.limit() && attempts < maxReadAttempts) {
          bytesRead += channel.read(_buffer)
          attempts += 1
        }

        if (bytesRead < _buffer.limit()) {
          throw new IOException(s"Read $bytesRead of ${_buffer.limit()} bytes from $position")
        }

        val dupe = ByteBuffer.allocate(_buffer.capacity())
        _buffer.clear()
        dupe.put(_buffer)
      }
    )

  def ensureBlocks(from: Long, to: Long): Unit =
    for {
      idx ← from to to
    } {
      getBlock(idx)
    }

  def read(start: Long, end: Long): ByteBuffer = {
    val startBlock = start / blockSize
    val endBlock = end / blockSize

    ensureBlocks(startBlock, endBlock)

    val buffer = ByteBuffer.allocate((end - start).toInt)

    for {
      idx ← startBlock to endBlock
      blockStart = idx * blockSize
      blockEnd = (idx + 1) * blockSize
      from = max((start - blockStart).toInt, 0)
      to = (min(end, blockEnd) - blockStart).toInt
      blockBuffer = blocks(idx)
      _ = blockBuffer.position(from)
    } {
      buffer.put(blockBuffer.array(), from, to - from)
    }

    buffer
  }
}
