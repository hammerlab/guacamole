package org.hammerlab.guacamole.reference

import org.hammerlab.genomics.reference.ContigName

case class ContigNotFound(contigName: ContigName, availableContigs: Iterable[ContigName])
  extends Exception(
    s"Contig $contigName does not exist in the current reference. Available contigs are:\n${availableContigs.mkString("\t", "\n\t", "\n")}"
  )
