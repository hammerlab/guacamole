package org.hammerlab.guacamole.filters.pileup

import org.hammerlab.guacamole.logging.DebugLogArgs
import org.hammerlab.guacamole.pileup.{ Pileup, PileupElement }
import org.kohsuke.args4j.{ Option => Args4jOption }

object PileupFilter {

  trait PileupFilterArguments extends DebugLogArgs {

    @Args4jOption(name = "--min-mapq", usage = "Minimum read mapping quality for a read (Phred-scaled). (default: 1)")
    var minAlignmentQuality: Int = 1

    @Args4jOption(name = "--filter-multi-allelic", usage = "Filter any pileups > 2 bases considered")
    var filterMultiAllelic: Boolean = false

    @Args4jOption(name = "--min-edge-distance", usage = "Filter reads where the base in the pileup is closer than minEdgeDistance to the (directional) end of the read")
    var minEdgeDistance: Int = 0

  }

  def apply(pileup: Pileup, args: PileupFilterArguments): Pileup =
    apply(
      pileup,
      args.filterMultiAllelic,
      args.minAlignmentQuality,
      args.minEdgeDistance
    )

  def apply(pileup: Pileup,
            filterMultiAllelic: Boolean,
            minAlignmentQuality: Int,
            minEdgeDistance: Int): Pileup = {

    var elements: Seq[PileupElement] = pileup.elements

    if (filterMultiAllelic) {
      elements = MultiAllelicPileupFilter(elements)
    }

    if (minAlignmentQuality > 0) {
      elements = QualityAlignedReadsFilter(elements, minAlignmentQuality)
    }

    if (minEdgeDistance > 0) {
      elements = EdgeBaseFilter(elements, minEdgeDistance)
    }

    Pileup(pileup.contigName, pileup.locus, pileup.contigSequence, elements)
  }
}
