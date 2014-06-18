package org.bdgenomics.guacamole.filters

import org.bdgenomics.guacamole.pileup.{ PileupElement, Pileup }
import org.bdgenomics.guacamole.Common.Arguments.Base
import org.kohsuke.args4j.Option
import org.bdgenomics.guacamole.Bases

/**
 * Filter to remove pileup elements with low alignment quality
 */
object QualityAlignedReadsFilter {
  /**
   *
   * @param elements sequence of pileup elements to filter
   * @param minimumAlignmentQuality Threshold to define whether a read was poorly aligned
   * @return filtered sequence of elements - those who had higher than minimumAlignmentQuality alignmentQuality
   */
  def apply(elements: Seq[PileupElement], minimumAlignmentQuality: Int): Seq[PileupElement] = {
    elements.filter(_.read.alignmentQuality > minimumAlignmentQuality)
  }
}

/**
 * This is a cheap computation for a region's complexity
 * We define a region's complexity as the number of reads whose alignment quality was below a certain threshold
 * (minimumAlignmentQuality).
 *
 * This filter eliminates pileups where the number of low quality aligned reads is above the maximumAlignmentComplexity
 */
object RegionComplexityFilter {

  /**
   *
   * @param elements sequence of pileup elements to filter
   * @param maximumAlignmentComplexity The maximum allowed percent of reads to be poorly aligned (as defined by minimumAlignmentQuality)
   * @param minimumAlignmentQuality Threshold to define whether a read was poorly aligned
   * @return Empty sequence if the number of poorly mapped reads surpass the maximumAlignmentComplexity
   */
  def apply(elements: Seq[PileupElement], maximumAlignmentComplexity: Int, minimumAlignmentQuality: Int): Seq[PileupElement] = {
    val depth = elements.length
    val qualityMappedReads = elements.filter(_.read.alignmentQuality > minimumAlignmentQuality).length
    if ((depth - qualityMappedReads) * 100.0 / depth > maximumAlignmentComplexity) {
      Seq.empty
    } else {
      elements
    }
  }
}

/**
 *
 */
object AmbiguousMappingPileupFilter {
  /**
   *
   * @param elements sequence of pileup elements to filter
   * @return Empty sequence if any of the reads had alignment quality = 0, otherwise original set of elements
   */
  def apply(elements: Seq[PileupElement]): Seq[PileupElement] = {
    if (!elements.forall(_.read.alignmentQuality > 0)) {
      Seq.empty
    } else {
      elements
    }
  }
}

/**
 * Filter to remove pileups which may produce multi-allelic variant calls.
 * These are generally more complex and more difficult to call
 */
object MultiAllelicPileupFilter {
  /**
   *
   * @param elements sequence of pileup elements to filter
   * @param maxPloidy pumber of alleles to expect (> maxPloidy would mean multiple possible alternates) default: 2
   * @return Empty sequence if there are > maxPloidy possible allelee, otherwise original set of elements
   */
  def apply(elements: Seq[PileupElement], maxPloidy: Int = 2): Seq[PileupElement] = {
    if (elements.map(el => Bases.basesToString(el.sequencedBases)).distinct.length > maxPloidy) {
      Seq.empty
    } else {
      elements
    }
  }
}

object PileupFilter {

  trait PileupFilterArguments extends Base {

    @Option(name = "-minMapQ", usage = "Minimum read mapping quality for a read (Phred-scaled). Default: 30")
    var minAlignmentQuality: Int = 30

    @Option(name = "-maxMappingComplexity", usage = "Maximum percent of reads that can be mapped with low quality (indicative of a complex region")
    var maxMappingComplexity: Int = 25

    @Option(name = "-filterAmbiguousMapped", usage = "Filter any reads with duplicate mapping or alignment quality = 0")
    var filterAmbiguousMapped: Boolean = false

    @Option(name = "-filterMultiAllelic", usage = "Filter any pileups > 2 bases considered")
    var filterMultiAllelic: Boolean = false

  }

  def apply(p: Pileup, args: PileupFilterArguments): Pileup = {

    var elements: Seq[PileupElement] = p.elements
    if (args.filterAmbiguousMapped) {
      elements = AmbiguousMappingPileupFilter(elements)
    }

    if (args.filterMultiAllelic) {
      elements = MultiAllelicPileupFilter(elements)
    }

    if (args.maxMappingComplexity < 100) {
      elements = RegionComplexityFilter(elements, args.maxMappingComplexity, args.minAlignmentQuality)
    }

    if (args.minAlignmentQuality > 0) {
      elements = QualityAlignedReadsFilter(elements, args.minAlignmentQuality)
    }

    Pileup(p.locus, elements)
  }
}
