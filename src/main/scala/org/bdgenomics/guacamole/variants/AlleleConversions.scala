package org.bdgenomics.guacamole.variants

import org.bdgenomics.formats.avro.{ GenotypeAllele, Genotype => ADAMGenotype }

import scala.collection.JavaConversions

/**
 * Note: ADAM Genotypes really map more to our concept of an "allele", hence some naming dissonance here.
 */
object AlleleConversions {

  implicit def calledAlleleToADAMGenotype(calledAllele: CalledAllele): Seq[ADAMGenotype] = {
    Seq(
      ADAMGenotype.newBuilder
        .setAlleles(JavaConversions.seqAsJavaList(Seq(GenotypeAllele.Ref, GenotypeAllele.Alt)))
        .setSampleId(calledAllele.sampleName.toCharArray)
        .setGenotypeQuality(calledAllele.evidence.phredScaledLikelihood)
        .setReadDepth(calledAllele.evidence.readDepth)
        .setExpectedAlleleDosage(
          calledAllele.evidence.alleleReadDepth.toFloat / calledAllele.evidence.readDepth
        )
        .setReferenceReadDepth(calledAllele.evidence.readDepth - calledAllele.evidence.alleleReadDepth)
        .setAlternateReadDepth(calledAllele.evidence.alleleReadDepth)
        .setVariant(calledAllele.adamVariant)
        .build
    )
  }

  implicit def calledSomaticAlleleToADAMGenotype(calledAllele: CalledSomaticAllele): Seq[ADAMGenotype] = {
    Seq(
      ADAMGenotype.newBuilder
        .setAlleles(JavaConversions.seqAsJavaList(Seq(GenotypeAllele.Ref, GenotypeAllele.Alt)))
        .setSampleId(calledAllele.sampleName.toCharArray)
        .setGenotypeQuality(calledAllele.phredScaledSomaticLikelihood)
        .setReadDepth(calledAllele.tumorEvidence.readDepth)
        .setExpectedAlleleDosage(
          calledAllele.tumorEvidence.alleleReadDepth.toFloat / calledAllele.tumorEvidence.readDepth
        )
        .setReferenceReadDepth(calledAllele.tumorEvidence.readDepth - calledAllele.tumorEvidence.alleleReadDepth)
        .setAlternateReadDepth(calledAllele.tumorEvidence.alleleReadDepth)
        .setVariant(calledAllele.adamVariant)
        .build
    )
  }
}

