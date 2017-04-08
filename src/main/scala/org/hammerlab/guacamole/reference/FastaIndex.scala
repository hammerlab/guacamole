package org.hammerlab.guacamole.reference

import org.hammerlab.genomics.bases.{ Base, Bases }
import org.hammerlab.genomics.reference.{ ContigName, ContigSequence, Locus, NumLoci }
import org.hammerlab.guacamole.reference.FastaIndex.Entry
import org.hammerlab.paths.Path

case class FastaIndex(entries: Map[ContigName, Entry]) {
  def apply(contigName: ContigName): Entry = entries(contigName)
}

object FastaIndex {

  case class Entry(contigName: ContigName,
                   length: NumLoci,
                   start: Long,
                   basesPerLine: Int,
                   bytesPerLine: Int) {
    val newlineBytes = bytesPerLine - basesPerLine

    def offset(locus: Locus): Long = {
      start + (locus.locus / basesPerLine) * bytesPerLine + (locus.locus % basesPerLine)
    }
  }

  object Entry {
    def apply(line: String): Entry = {
      line.split('\t') match {
        case Array(contigName, length, start, basesPerLine, bytesPerLine) ⇒
          Entry(contigName, NumLoci(length.toLong), start.toLong, basesPerLine.toInt, bytesPerLine.toInt)
        case _ ⇒
          throw new Exception(s"Invalid .fai entry line: $line")
      }
    }
  }

  def apply(path: Path): FastaIndex =
  path.extension match {
    case "fai" ⇒
      FastaIndex(
        path
          .lines
          .map(Entry(_))
          .map(entry ⇒ entry.contigName → entry)
          .toMap
      )
    case _ ⇒
      throw new Exception("need a fai")
  }
}
