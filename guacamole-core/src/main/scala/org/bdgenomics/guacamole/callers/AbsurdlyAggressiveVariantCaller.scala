package org.bdgenomics.guacamole.callers

import org.bdgenomics.adam.avro.{ADAMContig, ADAMVariant, ADAMGenotypeAllele, ADAMGenotype}
import org.bdgenomics.guacamole.{SlidingReadWindow, Pileup}
import scala.collection.immutable.NumericRange
import scala.collection.JavaConversions

class AbsurdlyAggressiveVariantCaller(samples: Set[String]) extends VariantCaller {

  val windowSize = 0

  def callVariants(reads: SlidingReadWindow, sortedLociToCall: Seq[NumericRange[Long]]): Iterator[ADAMGenotype] = {
    val lociIterator = sortedLociToCall.iterator.flatMap(_.toIterator)
    val pileupsIterator = Pileup.pileupsAtLoci(lociIterator, reads.setCurrentLocus _)
    pileupsIterator.flatMap(callVariantsAtLocus _)
  }

  private def callVariantsAtLocus(pileup: Pileup): Seq[ADAMGenotype] = {
    pileup.bySample.toSeq.flatMap({case (sampleName, samplePileup) =>
      val mismatches: Seq[Pileup.Element] = samplePileup.elements.filter(_.isMismatch)
      if (mismatches.isEmpty) {
        None
      } else {
        val (base: String, elements: Seq[Pileup.Element]) =
          mismatches.groupBy(_.sequenceRead).toSeq.sortBy(_._2.length).last
        Some(ADAMGenotype.newBuilder
          .setAlleles(JavaConversions.seqAsJavaList(List(ADAMGenotypeAllele.Alt, ADAMGenotypeAllele.Alt)))
          .setSampleId(sampleName.toCharArray)
          .setVariant(ADAMVariant.newBuilder
          .setPosition(pileup.locus)
          .setReferenceAllele(CharSequence(pileup.referenceBase))
          .setVariantAllele(base)
          .setContig(ADAMContig.newBuilder.setContigName(pileup.referenceName).build)
          ).build)
      }
    })
 }
}