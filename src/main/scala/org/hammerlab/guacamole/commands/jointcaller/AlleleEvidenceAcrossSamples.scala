package org.hammerlab.guacamole.commands.jointcaller

import org.hammerlab.guacamole.DistributedUtil._
import org.hammerlab.guacamole.commands.jointcaller.Input.{ Analyte, TissueType }
import org.hammerlab.guacamole.commands.jointcaller.PileupStats.AlleleMixture
import org.hammerlab.guacamole.pileup.Pileup

import scala.collection.Set

/**
 * Summarizes the evidence for a single allele across any number of samples.
 *
 * Keeps track of the evidence for the allele in each sample individually, and also the pooled normal and tumor DNA.
 *
 * @param parameters joint caller parameters
 * @param allele the allele under consideration
 * @param inputs metadata for each sample: is it tumor / normal, is it DNA / RNA
 * @param normalDNAPooledEvidence evidence for the pooled normal DNA samples
 * @param tumorDNAPooledEvidence evidence for the pooled tumor DNA samples
 * @param sampleEvidences evidences for each sample individually, corresponding to inputs
 */
case class AlleleEvidenceAcrossSamples(parameters: Parameters,
                                       allele: AlleleAtLocus,
                                       inputs: InputCollection,
                                       normalDNAPooledEvidence: NormalDNASampleAlleleEvidence,
                                       tumorDNAPooledEvidence: TumorDNASampleAlleleEvidence,
                                       sampleEvidences: PerSample[SampleAlleleEvidence]) {

  assume(inputs.items.map(_.index) == inputs.items.indices)

  /**
   * There are times when we want to treat all the per-sample evidences and pooled evidences together. We do this
   * with a sequence called `allEvidences` that has the evidence for each sample followed by that of the two pooled
   * samples. The normal dna pooled sample is found at index `normalDNAPooledIndex` and the tumor pooled is at index
   * `tumorDNAPooledIndex` in `allEvidences`.
   */
  def allEvidences: PerSample[SampleAlleleEvidence] =
    sampleEvidences ++ Seq(normalDNAPooledEvidence, tumorDNAPooledEvidence)
  def normalDNAPooledIndex = sampleEvidences.length
  def tumorDNAPooledIndex = normalDNAPooledIndex + 1

  /**
   * The individual TumorDNASampleAlleleEvidence and NormalDNASampleAlleleEvidence instances calculate the mixture
   * likelihoods, but do not know about the prior. We establish the germline prior here.
   *
   * The rationale for having this in the current class instead of the SampleAlleleEvidence classes is that eventually
   * we are going to use RNA evidence and phasing information to influence the posteriors, and we'll need to calculate
   * that here, since the individual evidence classes only know about a single sample.
   *
   * Note that at no point are we working with normalized priors, likelihoods, or posteriors. All of these are specified
   * just up to proportionality and do not need to sum to 1.
   *
   * @param alleles pair of alleles to return prior for
   * @return negative log-base 10 prior probability for that allele. Since log probs are negative, this number will
   *         be *positive* (it's the negative of a negative).
   */
  def germlinePrior(alleles: (String, String)): Double = Seq(alleles._1, alleles._2).count(_ == allele.ref) match {
    case 2                               => 0 // hom ref
    case 1                               => parameters.germlineNegativeLog10HeterozygousPrior // het
    case 0 if (alleles._1 == alleles._2) => parameters.germlineNegativeLog10HomozygousAlternatePrior // hom alt

    // Compound alt, which we should not hit in the current version of the caller (we currently only consider mixtures
    // involving a reference allele and a single alt allele)
    case 0                               => throw new AssertionError("Compound alts not supported")
  }

  /**
   * Log10 posterior probabilities for each possible germline genotype in each normal sample.
   *
   * Map is from input index (i.e. index into inputs and sampleEvidences) to mixture posteriors.
   *
   * The posteriors are plain log10 logprobs. They are negative. The maximum a posteriori estimate is the greatest
   * (i.e. least negative, closest to 0) posterior.
   */
  def perNormalSampleGermlinePosteriors: Map[Int, Map[(String, String), Double]] = {
    (inputs.items.filter(_.normalDNA).map(_.index) ++ Seq(normalDNAPooledIndex)).map(index => {
      val likelihoods = allEvidences(index).asInstanceOf[NormalDNASampleAlleleEvidence].logLikelihoods

      // These are not true posterior probabilities, but are proportional to posteriors.
      // Map from alleles to log probs.
      index -> likelihoods.map((kv => (kv._1, kv._2 - germlinePrior(kv._1))))
    }).toMap
  }
  def pooledGermlinePosteriors = perNormalSampleGermlinePosteriors(normalDNAPooledIndex)

  /**
   * Called germline genotype. We currently just use the maximum posterior from the pooled normal data.
   */
  def germlineAlleles: (String, String) = pooledGermlinePosteriors.maxBy(_._2)._1

  /** Are we making a germline call here? */
  def isGermlineCall = germlineAlleles != (allele.ref, allele.ref)

  /**
   * Negative log10 prior probability for a somatic call on a given mixture. See germlinePrior.
   */
  private def somaticPrior(mixture: Map[String, Double]): Double = {
    val contents = mixture.filter(_._2 > 0).keys.toSet
    if (contents == Set(allele.ref))
      0
    else if (contents == Set(allele.ref, allele.alt))
      parameters.somaticNegativeLog10VariantPrior
    else Double.MaxValue
  }

  /**
   * Log10 posterior probabilities for a somatic variant in each tumor sample. See perNormalSampleGermlinePosteriors.
   * *
   * @return Map {input index -> {{Allele -> Frequency} -> Posterior probability}
   */
  def perTumorSampleSomaticPosteriors: Map[Int, Map[AlleleMixture, Double]] = {
    (inputs.items.filter(_.tumorDNA).map(_.index) ++ Seq(tumorDNAPooledIndex)).map(index => {
      val likelihoods = allEvidences(index).asInstanceOf[TumorDNASampleAlleleEvidence].logLikelihoods
      index -> likelihoods.map(kv => (kv._1 -> (kv._2 - somaticPrior(kv._1))))
    }).toMap
  }

  /** Maximum a posteriori somatic mixtures for each tumor sample. */
  def perTumorSampleTopMixtures = perTumorSampleSomaticPosteriors.mapValues(_.maxBy(_._2)._1)

  /** Indices of tumor samples that triggered a call. */
  def tumorSampleIndicesTriggered: Seq[Int] = perTumorSampleTopMixtures
    .filter(pair => pair._2.keys.toSet != Set(germlineAlleles._1, germlineAlleles._2))
    .keys.toSeq

  /** Are we making a somatic call here? */
  def isSomaticCall = !isGermlineCall && tumorSampleIndicesTriggered.nonEmpty

  /** Are we making a germline or somatic call? */
  def isCall = isGermlineCall || isSomaticCall

}
object AlleleEvidenceAcrossSamples {

  /**
   * Collect evidence for a given allele across the provided pileups and return an AlleleEvidenceAcrossSamples
   * instance.
   *
   * One way to think about what we're doing here is we have a bunch of pileups, which contain reads and are therefore
   * large and not something we want to serialize and send around, and converting them into some sufficient statistics
   * for calling (or not calling) the variant indicated by `allele`. We can then send this object of statistics to the
   * driver where it can be used to write a VCF.
   *
   */
  def apply(
    parameters: Parameters,
    allele: AlleleAtLocus,
    pileups: PerSample[Pileup],
    inputs: InputCollection): AlleleEvidenceAcrossSamples = {

    val referenceSequence = pileups.head.referenceContigSequence.slice(allele.start.toInt, allele.end.toInt)

    val normalDNAPooledElements = inputs.normalDNA.map(input => pileups(input.index).elements).flatten
    val normalDNAPooledStats = PileupStats(normalDNAPooledElements, referenceSequence)
    val normalDNAPooledCharacterization = NormalDNASampleAlleleEvidence(allele, normalDNAPooledStats, parameters)

    val tumorDNAPooledElements = inputs.tumorDNA.map(input => pileups(input.index).elements).flatten
    val tumorDNAPooledStats = PileupStats(tumorDNAPooledElements, referenceSequence)
    val tumorDNAPooledCharacterization = TumorDNASampleAlleleEvidence(allele, tumorDNAPooledStats, parameters)

    val sampleEvidences = inputs.items.zip(pileups).map({
      case (input, pileup) =>
        val stats = PileupStats(pileup.elements, referenceSequence)
        (input.tissueType, input.analyte) match {
          case (TissueType.Normal, Analyte.DNA) => NormalDNASampleAlleleEvidence(allele, stats, parameters)
          case (TissueType.Tumor, Analyte.DNA)  => TumorDNASampleAlleleEvidence(allele, stats, parameters)
          // TODO: RNA
        }
    })
    AlleleEvidenceAcrossSamples(
      parameters,
      allele,
      inputs,
      normalDNAPooledCharacterization,
      tumorDNAPooledCharacterization,
      sampleEvidences)

  }
}
