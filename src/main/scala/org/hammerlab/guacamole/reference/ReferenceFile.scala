package org.hammerlab.guacamole.reference
import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel
import java.nio.file.Files

import grizzled.slf4j.Logging
import htsjdk.samtools.SAMException
import org.hammerlab.genomics.bases.{ Base, Bases }
import org.hammerlab.genomics.reference.{ ContigName, ContigSequence, Locus, NumLoci }
import org.hammerlab.guacamole.reference.FastaIndex.Entry
import org.hammerlab.paths.Path

import scala.math.min

case class ReferenceFile(path: Path, entries: Map[ContigName, Entry])
  extends ReferenceGenome
    with Logging {

  override def source: Option[Path] = Some(path)

  case class Contig(contigName: ContigName) extends ContigSequence {
    val entry = entries(contigName)
    var _stream: SeekableByteChannel = _
    var nextLocus = Locus(0)
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

    def stream(locus: Locus) = {
      if (_stream == null) {
        _stream = Files.newByteChannel(path)
        _stream.position(entry.offset(locus))
        nextLocus = locus
      } else if (locus != nextLocus) {
        _stream.position(entry.offset(locus))
        nextLocus = locus
      }
      _stream
    }

    override def apply(locus: Locus): Base = slice(locus, 1).head

    override def length: NumLoci = entry.length

    override def slice(start: Locus, length: Int): Bases = {
      val is = stream(start)

      var basesLeft = length

      if (start + length > entry.length)
        throw ContigLengthException(contigName, start, start + length, entry.length)

      var basesLeftInLine = entry.basesPerLine - (start.locus % entry.basesPerLine).toInt
      val builder = Bases.newBuilder
      while (basesLeft > 0) {
        val basesToRead = min(basesLeftInLine, basesLeft)

        val buf = buffer(basesToRead)

        val basesRead = is.read(buf)

        if (basesRead < basesToRead) {
          logger.info(
            s"Read $basesRead when expecting $basesToRead from $contigName:$nextLocus while fetching $contigName:$start-${start + length}"
          )
          buf.array().view.slice(0, basesRead).foreach(builder += _)
        } else
          buf.array().foreach(builder += _)

        basesLeft -= basesRead
        nextLocus += basesRead

        if (basesRead == basesLeftInLine) {
          newlineBuffer.clear()
          val bytesSkipped = is.read(newlineBuffer)
          if (bytesSkipped != newlineBytes)
            throw new SAMException(
              s"Skipped $bytesSkipped instead of $newlineBytes newline bytes at $contigName:$nextLocus (offset ${_stream.position()}) while fetching $contigName:$start-${start + length}"
            )
        }

        basesLeftInLine = entry.basesPerLine
      }

      builder.result()
    }
  }

  val contigs = collection.concurrent.TrieMap[ContigName, Contig]()
  override def apply(contigName: ContigName): ContigSequence =
    contigs.getOrElseUpdate(
      contigName,
      Contig(contigName)
    )
}

object ReferenceFile {
  def apply(path: Path): ReferenceFile =
    ReferenceFile(
      path,
      FastaIndex(Path(path.uri.toString + ".fai")).entries
    )
}
