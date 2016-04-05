package org.hammerlab.guacamole.commands.jointcaller

import org.hammerlab.guacamole.commands.jointcaller.PileupStats.AlleleMixture
import org.hammerlab.guacamole.commands.jointcaller.SampleAlleleEvidenceAnnotation._

/**
 * Summary of evidence for a particular germline allele in a single normal DNA sample.
 *
 * @param allele allele under consideration
 * @param allelicDepths Map from sequenced bases -> number of reads supporting that allele
 * @param logLikelihoods Map from germline genotypes to log10 likelihoods
 */
case class NormalDNASampleAlleleEvidence(allele: AlleleAtLocus,
                                         allelicDepths: Map[String, Int],
                                         logLikelihoods: Map[(String, String), Double],
                                         annotations: NamedAnnotations = emptyAnnotations) extends SampleAlleleEvidence {

  /** Total depth of all reads contributing an allele. */
  def depth(): Int = allelicDepths.values.sum

  /** Fraction of reads supporting this allele (variant allele frequency). */
  def vaf() = allelicDepths.getOrElse(allele.alt, 0).toDouble / depth

  override def withAnnotations(newAnnotations: NamedAnnotations): NormalDNASampleAlleleEvidence = {
    copy(annotations = annotations ++ newAnnotations)
  }
}
object NormalDNASampleAlleleEvidence {

  /** Given a pair of alleles describing a (germline) genotype, return the equivalent allele mixture. */
  def allelesToMixture(alleles: (String, String)): AlleleMixture = {
    if (alleles._1 == alleles._2)
      Map(alleles._1 -> 1.0)
    else
      Map(alleles._1 -> 0.5, alleles._2 -> 0.5)
  }

  /** Create a (serializable) NormalDNASampleAlleleEvidence instance from (non-serializable) pileup statistics. */
  def apply(allele: AlleleAtLocus, stats: PileupStats, parameters: Parameters): NormalDNASampleAlleleEvidence = {
    val possibleAllelePairs = Seq(
      (allele.ref, allele.ref), (allele.ref, allele.alt), (allele.alt, allele.alt))

    val logLikelihoods = possibleAllelePairs.map(allelePair => {
      allelePair -> stats.logLikelihoodPileup(allelesToMixture(allelePair))
    }).toMap

    val truncatedAllelicDepths = stats.truncatedAllelicDepths(parameters.maxAllelesPerSite + 1) // +1 for ref allele
    NormalDNASampleAlleleEvidence(allele, truncatedAllelicDepths, logLikelihoods)
  }
}