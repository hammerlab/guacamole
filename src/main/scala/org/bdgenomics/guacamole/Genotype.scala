package org.bdgenomics.guacamole

import org.bdgenomics.formats.avro.{ ADAMVariant, ADAMContig, ADAMGenotype, ADAMGenotypeAllele }
import scala.collection.JavaConversions
import org.bdgenomics.adam.util.PhredUtils

/**
 * A Genotype is a sequence of alleles of length equal to the ploidy of the organism.
 *
 * A Genotype is for a particular reference locus. Each allele gives the base(s) present on a chromosome at that
 * locus.
 *
 * For example, the possible single-base diploid genotypes are Seq('A', 'A'), Seq('A', 'T') ... Seq('T', 'T').
 * Alleles can also be multiple bases as well, e.g. Seq("AAA", "T")
 *
 */
case class Genotype(alleles: String*) {

  /**
   * The ploidy of the organism is the number of alleles in the genotype.
   */
  val ploidy = alleles.size

  lazy val uniqueAllelesCount = alleles.toSet.size

  def getNonReferenceAlleles(referenceAllele: String): Seq[String] = {
    alleles.filter(_ != referenceAllele)
  }

  /**
   * Counts alleles in this genotype that are not the same as the specified reference allele.
   *
   * @param referenceAllele Reference allele to compare against
   * @return Count of non reference alleles
   */
  def numberOfVariants(referenceAllele: String): Int = {
    getNonReferenceAlleles(referenceAllele).size
  }

  /**
   * Returns whether this genotype contains any non-reference alleles for a given reference sequence.
   *
   * @param referenceAllele Reference allele to compare against
   * @return True if at least one allele is not the reference
   */
  def isVariant(referenceAllele: String): Boolean = {
    numberOfVariants(referenceAllele) > 0
  }

  /**
   * Transform the alleles in this genotype to the ADAM allele enumeration.
   * Classifies alleles as Reference or Alternate.
   *
   * @param referenceAllele Reference allele to compare against
   * @return Sequence of GenotypeAlleles which are Ref, Alt or OtherAlt.
   */
  def getGenotypeAlleles(referenceAllele: String): Seq[ADAMGenotypeAllele] = {
    assume(ploidy == 2)
    val numVariants = numberOfVariants(referenceAllele)
    if (numVariants == 0) {
      Seq(ADAMGenotypeAllele.Ref, ADAMGenotypeAllele.Ref)
    } else if (numVariants > 0 && uniqueAllelesCount == 1) {
      Seq(ADAMGenotypeAllele.Alt, ADAMGenotypeAllele.Alt)
    } else if (numVariants >= 2 && uniqueAllelesCount > 1) {
      Seq(ADAMGenotypeAllele.Alt, ADAMGenotypeAllele.OtherAlt)
    } else {
      Seq(ADAMGenotypeAllele.Ref, ADAMGenotypeAllele.Alt)
    }
  }

}

object CalledGenotype {

  implicit def calledGenotypeToADAMGenotype(calledGenotype: CalledGenotype): Seq[ADAMGenotype] = {
    val genotypeAlleles = JavaConversions.seqAsJavaList(calledGenotype.alleles.getGenotypeAlleles(Bases.baseToString(calledGenotype.referenceBase)))
    calledGenotype.alleles.getNonReferenceAlleles(Bases.baseToString(calledGenotype.referenceBase)).map(
      variantAllele => {
        val variant = ADAMVariant.newBuilder
          .setPosition(calledGenotype.start)
          .setReferenceAllele(Bases.baseToString(calledGenotype.referenceBase))
          .setVariantAllele(variantAllele)
          .setContig(ADAMContig.newBuilder.setContigName(calledGenotype.referenceContig).build)
          .build
        ADAMGenotype.newBuilder
          .setAlleles(genotypeAlleles)
          .setSampleId(calledGenotype.sampleName.toCharArray)
          .setGenotypeQuality(calledGenotype.evidence.phredScaledLikelihood)
          .setReadDepth(calledGenotype.evidence.readDepth)
          .setExpectedAlleleDosage(calledGenotype.evidence.alternateReadDepth.toFloat / calledGenotype.evidence.readDepth)
          .setAlternateReadDepth(calledGenotype.evidence.alternateReadDepth)
          .setVariant(variant)
          .build
      })
  }

  implicit def calledSomaticGenotypeToADAMGenotype(calledGenotype: CalledSomaticGenotype): Seq[ADAMGenotype] = {
    val genotypeAlleles = JavaConversions.seqAsJavaList(calledGenotype.alleles.getGenotypeAlleles(Bases.baseToString(calledGenotype.referenceBase)))
    calledGenotype.alleles.getNonReferenceAlleles(Bases.baseToString(calledGenotype.referenceBase)).map(
      variantAllele => {
        val variant = ADAMVariant.newBuilder
          .setPosition(calledGenotype.start)
          .setReferenceAllele(Bases.baseToString(calledGenotype.referenceBase))
          .setVariantAllele(variantAllele)
          .setContig(ADAMContig.newBuilder.setContigName(calledGenotype.referenceContig).build)
          .build
        ADAMGenotype.newBuilder
          .setAlleles(genotypeAlleles)
          .setSampleId(calledGenotype.sampleName.toCharArray)
          .setGenotypeQuality(calledGenotype.tumorEvidence.phredScaledLikelihood)
          .setReadDepth(calledGenotype.tumorEvidence.readDepth)
          .setExpectedAlleleDosage(calledGenotype.tumorEvidence.alternateReadDepth.toFloat / calledGenotype.tumorEvidence.readDepth)
          .setAlternateReadDepth(calledGenotype.tumorEvidence.alternateReadDepth)
          .setVariant(variant)
          .build
      })
  }
}

case class CalledGenotype(sampleName: String,
                          referenceContig: String,
                          start: Long,
                          referenceBase: Byte,
                          alternateBase: String,
                          alleles: Genotype,
                          evidence: GenotypeEvidence,
                          length: Int = 1) extends HasReferenceRegion {
  val end = start + length
}

case class CalledSomaticGenotype(sampleName: String,
                                 referenceContig: String,
                                 start: Long,
                                 referenceBase: Byte,
                                 alternateBase: String,
                                 alleles: Genotype,
                                 tumorEvidence: GenotypeEvidence,
                                 normalEvidence: GenotypeEvidence,
                                 length: Int = 1) extends HasReferenceRegion {
  val end = start + length
}

case class GenotypeEvidence(likelihood: Double,
                            readDepth: Int,
                            alternateReadDepth: Int,
                            forwardDepth: Int,
                            alternateForwardDepth: Int) {

  lazy val phredScaledLikelihood = PhredUtils.successProbabilityToPhred(likelihood)

}
