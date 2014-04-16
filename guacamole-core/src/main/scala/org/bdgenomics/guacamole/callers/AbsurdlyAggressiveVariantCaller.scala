package org.bdgenomics.guacamole.callers

import org.bdgenomics.adam.avro.{ADAMContig, ADAMVariant, ADAMGenotypeAllele, ADAMGenotype}
import org.bdgenomics.guacamole.{PileupElement, SlidingReadWindow, Pileup}
import scala.collection.immutable.NumericRange

class AbsurdlyAggressiveVariantCaller(samples: Set[String]) extends VariantCaller {

  val windowSize = 0

  def callVariants(reads: SlidingReadWindow, sortedLociToCall: Seq[NumericRange[Long]]): Seq[ADAMGenotype] = {
    val lociIterator = sortedLociToCall.iterator.flatMap(_.toIterator)
    val pileupsIterator = Pileup.pileupsAtLoci(lociIterator, reads.setCurrentLocus _)
    pileupsIterator.flatMap(callVariantsAtLocus _)
  }

  private def callVariantsAtLocus(pileup: Pileup): Seq[ADAMGenotype] = {
    for {
      (sampleName, samplePileup) <- pileup.bySample
      mismatches: Seq[PileupElement] = samplePileup.elements.filter(_.isMismatch)
      if (!mismatches.isEmpty)
      (base: String, elements: Seq[PileupElement]) = mismatches.groupBy(_.sequenceRead).toSeq.sortBy(_._2.length).last
    } yield ADAMGenotype.newBuilder
      .setAlleles(List(ADAMGenotypeAllele.Alt, ADAMGenotypeAllele.Alt))
      .setSampleId(sampleName.toCharArray)
      .setVariant(ADAMVariant.newBuilder
        .setPosition(pileup.locus)
        .setReferenceAllele(pileup.referenceBase)
        .setVariantAllele(base)
        .setContig(ADAMContig.newBuilder.setContigName(pileup.referenceName).build)
      ).build
 }
}