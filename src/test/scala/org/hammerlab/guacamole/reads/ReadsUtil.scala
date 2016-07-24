package org.hammerlab.guacamole.reads

import org.scalatest.Matchers

trait ReadsUtil extends Matchers {
  def makeReads(reads: Seq[(String, Int, Int, Int)]): BufferedIterator[TestRegion] =
    (for {
      (contig, start, end, num) <- reads
      i <- 0 until num
    } yield
      TestRegion(contig, start, end)
    ).iterator.buffered
}
