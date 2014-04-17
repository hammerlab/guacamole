package org.bdgenomics.guacamole.callers

import org.bdgenomics.guacamole.{ Pileup, SlidingReadWindow }
import scala.collection.immutable.NumericRange
import org.bdgenomics.adam.avro.ADAMGenotype

/**
 * Trait for a variant caller implementation.
 */
trait VariantCaller {

  /**
   * The size of the sliding window (number of bases to either side of a locus) requested by this variant caller
   * implementation.
   *
   * Implementations must override this.
   *
   */
  val windowSize: Long

  /**
   * Given a [[SlidingReadWindow]] and a sequence of sorted loci ranges to call variants at, returns an iterator of
   * genotypes giving the result of variant calling. The [[SlidingReadWindow]] will have the window size requested
   * by this variant caller implementation.
   *
   * Implementations must override this.
   *
   */
  def callVariants(reads: SlidingReadWindow, sortedLociToCall: Seq[NumericRange[Long]]): Iterator[ADAMGenotype]
}
