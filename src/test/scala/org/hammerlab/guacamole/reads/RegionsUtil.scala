package org.hammerlab.guacamole.reads

import org.hammerlab.genomics.reference.{ ContigName, Locus, Region }

trait RegionsUtil {
  def makeRegions(reads: (ContigName, Locus, Locus, Int)*): Seq[Region] =
    for {
      (contigName, start, end, num) <- reads
      i ← 0 until num
    } yield
      Region(contigName, start, end)
}
