package org.hammerlab.guacamole.commands.jointcaller

import org.hammerlab.guacamole.commands.jointcaller.PileupStats.AlleleMixture

/**
 *
 * Summary of evidence for a particular somatic allele in a single tumor DNA sample.
 *
 * @param allele allele under consideration
 * @param allelicDepths Map from sequenced bases -> number of reads supporting that allele
 * @param logLikelihoods Map from allelic mixtures to log10 likelihoods
 */
case class TumorDNASampleAlleleEvidence(allele: AlleleAtLocus,
                                        allelicDepths: Map[String, Int],
                                        logLikelihoods: Map[AlleleMixture, Double])
    extends SampleAlleleEvidence {

  /** Total depth of all reads contributing an allele. */
  def depth(): Int = allelicDepths.values.sum

  /** Fraction of reads supporting this allele (variant allele frequency). */
  def vaf() = allelicDepths.getOrElse(allele.alt, 0).toDouble / depth

  /**
   * Apply a transformation function to the alleles. See AlleleAtLocus.transformAlleles for details.
   */
  def transformAlleles(alleleTransform: String => String,
                       startEndTransform: (Long, Long) => (Long, Long)): TumorDNASampleAlleleEvidence = {
    copy(
      allele = allele.transform(alleleTransform, startEndTransform),
      allelicDepths = allelicDepths.map(pair => alleleTransform(pair._1) -> pair._2),
      logLikelihoods = logLikelihoods.map(pair1 => {
        pair1._1.map(pair2 => alleleTransform(pair2._1) -> pair2._2) -> pair1._2
      }))
  }
}
object TumorDNASampleAlleleEvidence {

  /** Create a (serializable) TumorDNASampleAlleleEvidence instance from (non-serializable) pileup statistics. */
  def apply(allele: AlleleAtLocus, stats: PileupStats, parameters: Parameters): TumorDNASampleAlleleEvidence = {
    assume(allele.ref == stats.ref)

    val altVaf = math.max(parameters.somaticVafFloor, stats.vaf(allele.alt))
    val possibleMixtures = Seq(Map(allele.ref -> 1.0)) ++ (
      if (allele.ref != allele.alt)
        Seq(Map(allele.alt -> altVaf, allele.ref -> (1.0 - altVaf)))
      else
        Seq.empty)
    val logLikelihoods = possibleMixtures.map(mixture => mixture -> stats.logLikelihoodPileup(mixture)).toMap
    TumorDNASampleAlleleEvidence(allele, stats.allelicDepths, logLikelihoods)
  }
}