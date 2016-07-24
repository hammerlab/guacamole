package org.hammerlab.guacamole.pileup

import org.hammerlab.guacamole.reads.MappedRead
import org.hammerlab.guacamole.reference.{Locus, ReferenceBroadcast}

trait Util {

  def makePileup(reads: Seq[MappedRead],
                 locus: Locus = 2,
                 contigName: String = "chr1")(implicit reference: ReferenceBroadcast) =
    Pileup(reads, contigName, locus, reference.getContig(contigName))

  def makePileup(reads: Seq[MappedRead],
                 contigName: String,
                 locus: Locus)(implicit reference: ReferenceBroadcast) =
    Pileup(reads, contigName, locus, reference.getContig(contigName))
}
