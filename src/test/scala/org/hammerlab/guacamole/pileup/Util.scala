package org.hammerlab.guacamole.pileup

import org.hammerlab.guacamole.reads.MappedRead
import org.hammerlab.guacamole.reference.{ContigName, Locus, ReferenceBroadcast}

trait Util {

  def makePileup(reads: Seq[MappedRead],
                 locus: Locus = 2,
                 contigName: ContigName = "chr1")(implicit reference: ReferenceBroadcast) =
    Pileup(reads, contigName, locus, reference.getContig(contigName))

  def makePileup(reads: Seq[MappedRead],
                 contigName: ContigName,
                 locus: Locus)(implicit reference: ReferenceBroadcast) =
    Pileup(reads, contigName, locus, reference.getContig(contigName))
}
