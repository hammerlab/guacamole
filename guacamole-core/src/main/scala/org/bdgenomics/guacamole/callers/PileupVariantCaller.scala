package org.bdgenomics.guacamole.callers

import org.bdgenomics.guacamole.{ Pileup, LociSet, SlidingReadWindow }
import org.bdgenomics.adam.avro.ADAMGenotype

/**
 * A [[PileupVariantCaller]] is a [[SlidingWindowVariantCaller]] that examines only a single pileup at each locus.
 *
 */
trait PileupVariantCaller extends SlidingWindowVariantCaller {

  // We consider each locus independently of all others.
  override val halfWindowSize = 0L

  override def callVariants(samples: Seq[String], reads: SlidingReadWindow, loci: LociSet.SingleContig): Iterator[ADAMGenotype] = {
    val lociAndReads = loci.individually.map(locus => (locus, reads.setCurrentLocus(locus)))
    val pileupsIterator = Pileup.pileupsAtLoci(lociAndReads)
    pileupsIterator.flatMap(callVariantsAtLocus _)
  }

  /**
   * Implementations must override this.
   * @param pileup The Pileup at a particular locus.
   * @return ADAMGenotype instances for the called variants, if any.
   */
  def callVariantsAtLocus(pileup: Pileup): Seq[ADAMGenotype]
}
