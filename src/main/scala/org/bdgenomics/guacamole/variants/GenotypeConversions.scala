package org.bdgenomics.guacamole.variants

import org.bdgenomics.formats.avro.{ GenotypeAllele, Genotype => ADAMGenotype }

import scala.collection.JavaConversions

object GenotypeConversions {

  implicit def calledGenotypeToADAMGenotype(calledGenotype: CalledGenotype): Seq[ADAMGenotype] = {
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

  implicit def calledSomaticGenotypeToADAMGenotype(calledGenotype: CalledSomaticGenotype): Seq[ADAMGenotype] = {
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

