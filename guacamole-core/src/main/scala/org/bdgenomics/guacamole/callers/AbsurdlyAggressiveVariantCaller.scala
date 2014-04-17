package org.bdgenomics.guacamole.callers

import org.bdgenomics.adam.avro.{ ADAMContig, ADAMVariant, ADAMGenotypeAllele, ADAMGenotype }
import org.bdgenomics.guacamole.{ LociSet, SlidingReadWindow, Pileup }
import scala.collection.immutable.NumericRange
import scala.collection.JavaConversions

/**
 * Example variant caller implementation.
 *
 * Eschews math, and calls a variant whenever there is any evidence at all for it.
 *
 * @param samples The sample names to call variants for.
 */
class AbsurdlyAggressiveVariantCaller(samples: Set[String]) extends VariantCaller {

  override val windowSize: Long = 0

  override def callVariants(reads: SlidingReadWindow, loci: LociSet.SingleContig): Iterator[ADAMGenotype] = {
    val lociAndReads = loci.individually.map(locus => (locus, reads.setCurrentLocus(locus)))
    val pileupsIterator = Pileup.pileupsAtLoci(lociAndReads)
    pileupsIterator.flatMap(callVariantsAtLocus _)
  }

  private def callVariantsAtLocus(pileup: Pileup): Seq[ADAMGenotype] = {
    pileup.bySample.toSeq.flatMap({
      case (sampleName, samplePileup) =>
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
              .setReferenceAllele(pileup.referenceBase.toString)
              .setVariantAllele(base)
              .setContig(ADAMContig.newBuilder.setContigName(pileup.referenceName).build)
              .build)
            .build)
        }
    })
  }
}