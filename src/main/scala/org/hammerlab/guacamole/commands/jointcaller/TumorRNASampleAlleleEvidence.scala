package org.hammerlab.guacamole.commands.jointcaller

import org.hammerlab.guacamole.commands.jointcaller.PileupStats.AlleleMixture
import org.hammerlab.guacamole.commands.jointcaller.SampleAlleleEvidenceAnnotation._

/**
 *
 * Summary of evidence for a particular somatic allele in a single tumor RNA sample.
 *
 * @param allele allele under consideration
 * @param allelicDepths Map from sequenced bases -> number of reads supporting that allele
 * @param logLikelihoods Map from allelic mixtures to log10 likelihoods
 */
case class TumorRNASampleAlleleEvidence(allele: AlleleAtLocus,
                                        allelicDepths: Map[String, Int],
                                        logLikelihoods: Map[AlleleMixture, Double],
                                        annotations: NamedAnnotations = emptyAnnotations)
    extends SampleAlleleEvidence {

  /** Total depth of all reads contributing an allele. */
  def depth(): Int = allelicDepths.values.sum

  /** Fraction of reads supporting this allele (variant allele frequency). */
  def vaf() = allelicDepths.getOrElse(allele.alt, 0).toDouble / depth

  override def withAnnotations(newAnnotations: NamedAnnotations): TumorRNASampleAlleleEvidence = {
    copy(annotations = annotations ++ newAnnotations)
  }
}
object TumorRNASampleAlleleEvidence {

  /** Create a (serializable) TumorRNASampleAlleleEvidence instance from (non-serializable) pileup statistics. */
  def apply(allele: AlleleAtLocus, stats: PileupStats, parameters: Parameters): TumorRNASampleAlleleEvidence = {
    assume(allele.ref == stats.ref, "%s != %s".format(allele.ref, stats.ref))

    val altVaf = math.max(parameters.somaticVafFloor, stats.vaf(allele.alt))
    val possibleMixtures = Seq(Map(allele.ref -> 1.0)) ++ (
      if (allele.ref != allele.alt)
        Seq(Map(allele.alt -> altVaf, allele.ref -> (1.0 - altVaf)))
      else
        Seq.empty)
    val logLikelihoods = possibleMixtures.map(mixture => mixture -> stats.logLikelihoodPileup(mixture)).toMap
    val truncatedAllelicDepths = stats.truncatedAllelicDepths(parameters.maxAllelesPerSite + 1) // +1 for ref allele
    TumorRNASampleAlleleEvidence(allele, truncatedAllelicDepths, logLikelihoods)
  }
}