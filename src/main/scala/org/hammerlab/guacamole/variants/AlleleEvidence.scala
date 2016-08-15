package org.hammerlab.guacamole.variants

import breeze.linalg.DenseVector
import breeze.stats.{ mean, median }
import org.bdgenomics.adam.util.PhredUtils
import org.hammerlab.guacamole.pileup.Pileup

/**
 *
 * Sample specific pileup and read statistics in support of a given variant
 *
 * @param likelihood probability of the genotype
 * @param readDepth total reads at the genotype position
 * @param alleleReadDepth total reads with allele base at the genotype position
 * @param forwardDepth total reads on the forward strand at the genotype position
 * @param alleleForwardDepth total reads with allele base on the forward strand at the genotype position
 * @param meanMappingQuality mean mapping quality of reads
 * @param medianMappingQuality median mapping quality of reads
 * @param meanBaseQuality mean base quality of bases covering this position
 * @param medianBaseQuality median base quality of bases covering this position
 * @param medianMismatchesPerRead median number of mismatches on reads supporting this variant
 */
case class AlleleEvidence(likelihood: Double,
                          readDepth: Int,
                          alleleReadDepth: Int,
                          forwardDepth: Int,
                          alleleForwardDepth: Int,
                          meanMappingQuality: Double,
                          medianMappingQuality: Double,
                          meanBaseQuality: Double,
                          medianBaseQuality: Double,
                          medianMismatchesPerRead: Double) {

  lazy val phredScaledLikelihood = PhredUtils.successProbabilityToPhred(likelihood - 1e-10) //subtract small delta to prevent p = 1
  lazy val variantAlleleFrequency = alleleReadDepth.toFloat / readDepth
}

object AlleleEvidence {

  def apply(likelihood: Double,
            allele: Allele,
            alleleReadDepth: Int,
            allelePositiveReadDepth: Int,
            pileup: Pileup): AlleleEvidence = {

    val alleleElements = pileup.elements.filter(_.allele == allele)
    val alignmentScores = DenseVector(alleleElements.map(_.read.alignmentQuality.toDouble).toArray)
    val baseQualityScores = DenseVector(alleleElements.map(_.qualityScore.toDouble).toArray)

    if (alleleElements.isEmpty)
      AlleleEvidence(
        likelihood,
        pileup.depth,
        alleleReadDepth,
        pileup.positiveDepth,
        allelePositiveReadDepth,
        meanMappingQuality = Double.NaN,
        medianMappingQuality = Double.NaN,
        meanBaseQuality = Double.NaN,
        medianBaseQuality = Double.NaN,
        medianMismatchesPerRead = Double.NaN
      )
    else
      AlleleEvidence(
        likelihood,
        pileup.depth,
        alleleReadDepth,
        pileup.positiveDepth,
        allelePositiveReadDepth,
        meanMappingQuality = mean(alignmentScores),
        medianMappingQuality = median(alignmentScores),
        meanBaseQuality = mean(baseQualityScores),
        medianBaseQuality = median(baseQualityScores),
        medianMismatchesPerRead = median(
          DenseVector(alleleElements.map(_.read.countOfMismatches(pileup.contigSequence)).toArray)))
  }

  def apply(likelihood: Double, allele: Allele, pileup: Pileup): AlleleEvidence = {
    val (alleleReadDepth, allelePositiveReadDepth) = pileup.alleleReadDepthAndPositiveDepth(allele)
    AlleleEvidence(likelihood, allele, alleleReadDepth, allelePositiveReadDepth, pileup)

  }
}
