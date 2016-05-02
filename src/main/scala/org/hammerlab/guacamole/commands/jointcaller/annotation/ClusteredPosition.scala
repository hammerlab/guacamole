package org.hammerlab.guacamole.commands.jointcaller.annotation

/**
  * Reject false positives caused by misalignments hallmarked by the alternate alleles being clustered
  * at a consistent distance from the start or end of the read alignment. We calculate the median and
  * median absolute deviation of the distance from both the start and end of the read and reject sites
  * that have a median ≤ 10 (near the start/end of the alignment) and a median absolute deviation ≤ 3
  * (clustered).
  */
case class ClusteredPosition() {

}

object ClusteredPosition
