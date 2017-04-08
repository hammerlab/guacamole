package org.hammerlab.guacamole.reference

import org.hammerlab.genomics.reference.{ ContigName, Locus, NumLoci }

case class ContigLengthException(contigName: ContigName,
                                 start: Locus,
                                 end: Locus,
                                 length: NumLoci)
  extends Exception(
    s"Attempted to read from $start to $end of contig $contigName of length $length"
  )
