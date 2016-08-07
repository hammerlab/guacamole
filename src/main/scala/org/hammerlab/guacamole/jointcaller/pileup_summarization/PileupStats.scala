package org.hammerlab.guacamole.jointcaller.pileup_summarization

import org.bdgenomics.adam.util.PhredUtils
import org.hammerlab.guacamole.jointcaller.pileup_summarization.PileupStats.AlleleMixture
import org.hammerlab.guacamole.pileup.PileupElement
import org.hammerlab.guacamole.util.Bases

/**
 * Statistics over a PileupElement instances (a pileup).
 *
 * This is not a case class because we do not intend for it to be serialized, and we want to get an error if we try.
 *
 * We can consider sequenced alleles of any reference length. For example, if we are evaluating support for a 10-base
 * deletion, we want the keys returned by `allelicDepths` method to be the sequenced alleles aligning to that
 * 10-base region; in particular the reference allele would be length 10. The length of the `refSequence` attribute
 * establishes the reference length to be considered.
 *
 * @param elements elements from a pileup. They should all be positioned at the same locus. The alleles considered will
 *                 align at this locus + 1.
 * @param referenceSequence reference bases. The length determines the size of alleles to consider. The first element should
 *                    be the reference base at locus elements.head.locus + 1.
 */
class PileupStats(val elements: Seq[PileupElement], val referenceSequence: Seq[Byte]) {
  assume(referenceSequence.nonEmpty)
  assume(elements.forall(_.locus == elements.head.locus))

  /** The reference sequence as a string. */
  val ref = Bases.basesToString(referenceSequence)

  /**
   * The sequenced alleles at this site as ReadSubsequence instances.
   *
   * Every read that is "anchored" on either side of the reference region by a matching, non-variant base is represented
   * here.
   */
  val subsequences: Seq[ReadSubsequence] = elements.flatMap(
    element => ReadSubsequence.ofFixedReferenceLength(element, referenceSequence.length))

  /** Map from sequenced allele -> the ReadSubsequence instances for that allele. */
  val alleleToSubsequences: Map[String, Seq[ReadSubsequence]] = subsequences.groupBy(_.sequence)

  /** Map from sequenced allele -> number of reads supporting that allele. */
  val allelicDepths = alleleToSubsequences.mapValues(_.size).withDefaultValue(0)

  def truncatedAllelicDepths(max: Int): Map[String, Int] = {
    allelicDepths.toSeq.sortBy(-1 * _._2).zipWithIndex.filter(_._2 < max).map(_._1).toMap
  }

  /** Total depth, including reads that are NOT "anchored" by matching, non-variant bases. */
  val totalDepthIncludingReadsContributingNoAlleles = elements.size

  /** All sequenced alleles that are not the ref allele, sorted by decreasing allelic depth. */
  val nonRefAlleles: Seq[String] = allelicDepths.filterKeys(_ != ref).toSeq.sortBy(_._2 * -1).map(_._1)

  /** Alt allele with most reads. */
  val topAlt = nonRefAlleles.headOption.getOrElse("N")

  /** Alt allele with second-most reads. */
  val secondAlt = if (nonRefAlleles.size > 1) nonRefAlleles(1) else "N"

  /** Fraction of reads supporting the given allele. */
  def vaf(allele: String): Double = allelicDepths(allele).toDouble / totalDepthIncludingReadsContributingNoAlleles

  /** Map from allele to read names supporting that allele. */
  lazy val readNamesByAllele = alleleToSubsequences
    .mapValues(_.filter(_.read.alignmentQuality > 0).map(_.read.name).toSet)
    .withDefaultValue(Set.empty)

  /**
   * Compute likelihood P(data|mixture) of the sequenced bases (data) given the specified mixture.
   *
   * Reads with mapping quality 0 are ignored.
   *
   * @param mixture Map from sequenced allele -> variant allele fraction
   * @return log10 likelihood probability, always non-positive
   */
  def logLikelihoodPileup(mixture: AlleleMixture): Double = {
    def logLikelihoodReadSubsequence(subsequence: ReadSubsequence): Double = {
      if (subsequence.read.alignmentQuality == 0) {
        0.0
      } else {
        val mixtureFrequency = mixture.getOrElse(subsequence.sequence, 0.0)
        val probabilityCorrect = PhredUtils.phredToSuccessProbability(
          subsequence.meanBaseQuality.round.toInt) * subsequence.read.alignmentLikelihood
        val loglikelihood = math.log10(
          mixtureFrequency * probabilityCorrect + (1 - mixtureFrequency) * (1 - probabilityCorrect))
        loglikelihood
      }
    }
    subsequences.map(logLikelihoodReadSubsequence).sum
  }
}
object PileupStats {
  /** Map from sequenced allele -> variant allelic fraction. The allelic fractions should sum to 1. */
  type AlleleMixture = Map[String, Double]

  /** Create a PileupStats instance. */
  def apply(elements: Seq[PileupElement], refSequence: Seq[Byte]): PileupStats = {
    new PileupStats(elements, refSequence)
  }
}
