package org.hammerlab.guacamole.reference

import java.io.ByteArrayInputStream
import java.nio.ByteBuffer
import java.nio.file.Files

import grizzled.slf4j.Logging
import org.hammerlab.genomics.bases.{ Base, Bases }
import org.hammerlab.genomics.reference.{ ContigName, ContigSequence, Locus, NumLoci }
import org.hammerlab.guacamole.reference.FastaIndex.Entry
import org.hammerlab.paths.Path

import scala.math.min

case class Contig(contigName: ContigName,
                  entry: Entry,
                  path: Path,
                  blockSize: Int = 2 * 1024 * 1024)
  extends ContigSequence
    with Logging {

  lazy val channel = {
    val fileChannel = Files.newByteChannel(path)
    CachingChannel(fileChannel, blockSize)
  }

  val newlineBytes = entry.newlineBytes
  val bytesPerLine = entry.bytesPerLine

  val _buffers = collection.concurrent.TrieMap[Int, ByteBuffer]()

  def buffer(size: Int): ByteBuffer = {
    val buf =
      _buffers.getOrElseUpdate(
        size,
        ByteBuffer.allocate(size)
      )

    buf.clear()
    buf
  }

  val newlineBuffer = buffer(newlineBytes)

  override def apply(locus: Locus): Base = slice(locus, 1).head

  override def length: NumLoci = entry.length

  override def slice(start: Locus, length: Int): Bases = {
    if (start + length > entry.length)
      throw ContigLengthException(contigName, start, start + length, entry.length)

    val byteStart = entry.offset(start)
    val byteEnd = entry.offset(start + length)

    val bytes = channel.read(byteStart, byteEnd)

    val bais = new ByteArrayInputStream(bytes.array())

    var basesLeft = length

    var basesLeftInLine = entry.basesPerLine - (start.locus % entry.basesPerLine).toInt
    val builder = Bases.newBuilder
    while (basesLeft > 0) {
      val basesToRead = min(basesLeftInLine, basesLeft)

      var basesLeftToRead = basesToRead
      while (basesLeftToRead > 0) {
        builder += bais.read().toByte
        basesLeftToRead -= 1
      }

      basesLeft -= basesToRead

      if (basesToRead == basesLeftInLine) {
        var bytesToSkip = 0
        while (bytesToSkip < newlineBytes) {
          bais.read()
          bytesToSkip += 1
        }

        basesLeftInLine = entry.basesPerLine
      }
    }

    builder.result()
  }
}

