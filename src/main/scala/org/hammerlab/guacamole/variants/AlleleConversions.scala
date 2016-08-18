package org.hammerlab.guacamole.variants

import org.bdgenomics.formats.avro.{ GenotypeAllele, Genotype => ADAMGenotype }

import scala.collection.JavaConversions

/**
 * Note: ADAM Genotypes really map more to our concept of an "allele", hence some naming dissonance here.
 */
object AlleleConversions {

  def calledAlleleToADAMGenotype(calledAllele: CalledAllele): Seq[ADAMGenotype] = {
    Seq(
      ADAMGenotype.newBuilder
        .setAlleles(JavaConversions.seqAsJavaList(Seq(GenotypeAllele.Ref, GenotypeAllele.Alt)))
        .setSampleId(calledAllele.sampleName)
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

  def calledSomaticAlleleToADAMGenotype(calledAllele: CalledSomaticAllele): Seq[ADAMGenotype] = {
    Seq(
      ADAMGenotype.newBuilder
        .setAlleles(JavaConversions.seqAsJavaList(Seq(GenotypeAllele.Ref, GenotypeAllele.Alt)))
        .setSampleId(calledAllele.sampleName)
        .setGenotypeQuality(calledAllele.phredScaledSomaticLikelihood)
        .setReadDepth(calledAllele.tumorVariantEvidence.readDepth)
        .setExpectedAlleleDosage(
          calledAllele.tumorVariantEvidence.alleleReadDepth.toFloat / calledAllele.tumorVariantEvidence.readDepth
        )
        .setReferenceReadDepth(calledAllele.tumorVariantEvidence.readDepth - calledAllele.tumorVariantEvidence.alleleReadDepth)
        .setAlternateReadDepth(calledAllele.tumorVariantEvidence.alleleReadDepth)
        .setVariant(calledAllele.adamVariant)
        .build
    )
  }
}

