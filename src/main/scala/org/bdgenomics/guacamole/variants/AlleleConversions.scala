package org.bdgenomics.guacamole.variants

import org.bdgenomics.formats.avro.{ GenotypeAllele, Genotype => ADAMGenotype }

import scala.collection.JavaConversions

/**
 * Note: ADAM Genotypes really map more to our concept of an "allele", hence some naming dissonance here.
 */
object AlleleConversions {

  implicit def calledAlleleToADAMGenotype(calledGenotype: CalledAllele): Seq[ADAMGenotype] = {
    Seq(
      ADAMGenotype.newBuilder
        .setAlleles(JavaConversions.seqAsJavaList(Seq(GenotypeAllele.Ref, GenotypeAllele.Alt)))
        .setSampleId(calledGenotype.sampleName.toCharArray)
        .setGenotypeQuality(calledGenotype.evidence.phredScaledLikelihood)
        .setReadDepth(calledGenotype.evidence.readDepth)
        .setExpectedAlleleDosage(
          calledGenotype.evidence.alleleReadDepth.toFloat / calledGenotype.evidence.readDepth
        )
        .setAlternateReadDepth(calledGenotype.evidence.alleleReadDepth)
        .setVariant(calledGenotype.adamVariant)
        .build
    )
  }

  implicit def calledSomaticAlleleToADAMGenotype(calledGenotype: CalledSomaticAllele): Seq[ADAMGenotype] = {
    Seq(
      ADAMGenotype.newBuilder
        .setAlleles(JavaConversions.seqAsJavaList(Seq(GenotypeAllele.Ref, GenotypeAllele.Alt)))
        .setSampleId(calledGenotype.sampleName.toCharArray)
        .setGenotypeQuality(calledGenotype.phredScaledSomaticLikelihood)
        .setReadDepth(calledGenotype.tumorEvidence.readDepth)
        .setExpectedAlleleDosage(
          calledGenotype.tumorEvidence.alleleReadDepth.toFloat / calledGenotype.tumorEvidence.readDepth
        )
        .setAlternateReadDepth(calledGenotype.tumorEvidence.alleleReadDepth)
        .setVariant(calledGenotype.adamVariant)
        .build
    )
  }
}

