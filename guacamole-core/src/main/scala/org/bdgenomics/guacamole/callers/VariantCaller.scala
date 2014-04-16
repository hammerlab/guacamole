package org.bdgenomics.guacamole.callers

import org.bdgenomics.guacamole.{ Pileup, SlidingReadWindow }
import scala.collection.immutable.NumericRange
import org.bdgenomics.adam.avro.ADAMGenotype

trait VariantCaller {
  val windowSize: Long
  def callVariants(reads: SlidingReadWindow, sortedLociToCall: Seq[NumericRange[Long]]): Iterator[ADAMGenotype]
}
