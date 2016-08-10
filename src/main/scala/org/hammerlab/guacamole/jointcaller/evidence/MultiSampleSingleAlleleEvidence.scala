package org.hammerlab.guacamole.jointcaller.evidence

import org.hammerlab.guacamole.jointcaller.Input.{Analyte, TissueType}
import org.hammerlab.guacamole.jointcaller._
import org.hammerlab.guacamole.jointcaller.annotation.{MultiSampleAnnotations, SingleSampleAnnotations}
import org.hammerlab.guacamole.jointcaller.pileup_summarization.MultiplePileupStats
import org.hammerlab.guacamole.jointcaller.pileup_summarization.PileupStats.AlleleMixture
import org.hammerlab.guacamole.readsets.{PerSample, SampleId}

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
case class MultiSampleSingleAlleleEvidence(parameters: Parameters,
                                           allele: AlleleAtLocus,
                                           inputs: InputCollection,
                                           normalDNAPooledEvidence: NormalDNASingleSampleSingleAlleleEvidence,
                                           tumorDNAPooledEvidence: TumorDNASingleSampleSingleAlleleEvidence,
                                           sampleEvidences: PerSample[SingleSampleSingleAlleleEvidence],
                                           annotations: Option[MultiSampleAnnotations]) {

  assume(inputs.items.map(_.index) == inputs.items.indices)

  /**
   * There are times when we want to treat all the per-sample evidences and pooled evidences together. We do this
   * with a sequence called `allEvidences` that has the evidence for each sample followed by that of the two pooled
   * samples. The normal dna pooled sample is found at index `normalDNAPooledIndex` and the tumor pooled is at index
   * `tumorDNAPooledIndex` in `allEvidences`.
   */
  val allEvidences: Vector[SingleSampleSingleAlleleEvidence] =
    sampleEvidences.toVector ++
      Vector(
        normalDNAPooledEvidence,
        tumorDNAPooledEvidence
      )

  val normalDNAPooledIndex = sampleEvidences.length

  val tumorDNAPooledIndex = normalDNAPooledIndex + 1

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
  def germlinePrior(alleles: (String, String)): Double =
    Seq(
      alleles._1,
      alleles._2
    ).count(_ == allele.ref) match {
      case 2                               => 0  // hom ref
      case 1                               => parameters.germlineNegativeLog10HeterozygousPrior  // het
      case 0 if (alleles._1 == alleles._2) => parameters.germlineNegativeLog10HomozygousAlternatePrior  // hom alt

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
  val perNormalSampleGermlinePosteriors: Map[SampleId, Map[(String, String), Double]] = {
    (
      inputs
        .items
        .filter(_.normalDNA)
        .map(_.index) ++
        Seq(normalDNAPooledIndex)
    ).map(
      index => {

        val likelihoods =
          allEvidences(index)
            .asInstanceOf[NormalDNASingleSampleSingleAlleleEvidence]
            .logLikelihoods

        // These are not true posterior probabilities, but are proportional to posteriors.
        // Map from alleles to log probs.
        index ->
          (
            for {
              (alleles, likelihood) <- likelihoods
            } yield
              alleles -> (likelihood - germlinePrior(alleles))
          )
      }
    ).toMap
  }
  val pooledGermlinePosteriors = perNormalSampleGermlinePosteriors(normalDNAPooledIndex)

  /**
   * Called germline genotype. We currently just use the maximum posterior from the pooled normal data.
   */
  val germlineAlleles: (String, String) = pooledGermlinePosteriors.maxBy(_._2)._1

  /** Are we making a germline call here? */
  val isGermlineCall = germlineAlleles != (allele.ref, allele.ref)

  /** Negative log10 prior probability for the given mixture in RNA. */
  private def somaticPriorRna(mixture: Map[String, Double]): Double = {
    val contents = mixture.filter(_._2 > 0).keys.toSet
    if (contents == Set(allele.ref))
      0
    else if (contents == Set(allele.ref, allele.alt))
      parameters.somaticNegativeLog10VariantPriorRna
    else
      Double.MaxValue
  }

  /** Log10 posterior probabilities for a somatic variant in each tumor RNA sample.  */
  val perTumorRnaSampleSomaticPosteriors: Map[SampleId, Map[AlleleMixture, Double]] = {
    inputs
      .items
      .filter(_.tumorRNA)
      .map(
        input => {
          val likelihoods =
            allEvidences(input.index)
              .asInstanceOf[TumorRNASingleSampleSingleAlleleEvidence]
              .logLikelihoods

          input.index ->
            (
              for {
                (alleles, likelihood) <- likelihoods
              } yield
                alleles -> (likelihood - somaticPriorRna(alleles))
            )
        }
      ).toMap
  }

  /** Maximum a posteriori somatic mixtures for each tumor sample. */
  val perTumorRnaSampleTopMixtures = perTumorRnaSampleSomaticPosteriors.mapValues(_.maxBy(_._2)._1)

  /** Indices of tumor rna samples with expression */
  val tumorRnaSampleExpressed: PerSample[SampleId] =
    perTumorRnaSampleTopMixtures
      .filter(pair => pair._2.keys.toSet != Set(germlineAlleles._1, germlineAlleles._2))
      .keys
      .toVector

  /**
   * Negative log10 prior probability for a somatic call on a given mixture. See germlinePrior.
   */
  private def somaticPriorDna(mixture: Map[String, Double]): Double = {
    val contents = mixture.filter(_._2 > 0).keys.toSet
    if (contents == Set(allele.ref))
      0
    else if (contents == Set(allele.ref, allele.alt))
      if (tumorRnaSampleExpressed.nonEmpty)
        parameters.somaticNegativeLog10VariantPriorWithRnaEvidence  // we have RNA evidence so use less stringent prior
      else
        parameters.somaticNegativeLog10VariantPrior
    else
      Double.MaxValue
  }

  /**
   * Log10 posterior probabilities for a somatic variant in each tumor DNA sample. See perNormalSampleGermlinePosteriors.
   *
   * @return Map {input index -> {{Allele -> Frequency} -> Posterior probability}
   */
  val perTumorDnaSampleSomaticPosteriors: Map[SampleId, Map[AlleleMixture, Double]] = {
    (
      inputs
        .items
        .filter(_.tumorDNA)
        .map(_.index) ++
        Seq(tumorDNAPooledIndex)
    ).map(
      index => {

        val likelihoods =
          allEvidences(index)
            .asInstanceOf[TumorDNASingleSampleSingleAlleleEvidence]
            .logLikelihoods

        index ->
          (
            for {
              (alleles, likelihood) <- likelihoods
            } yield
              alleles -> (likelihood - somaticPriorDna(alleles))
          )
      }
    ).toMap
  }

  /** Maximum a posteriori somatic mixtures for each tumor sample. */
  val perTumorDnaSampleTopMixtures = perTumorDnaSampleSomaticPosteriors.mapValues(_.maxBy(_._2)._1)

  /** Indices of tumor samples that triggered a call. */
  val tumorDnaSampleIndicesTriggered: PerSample[SampleId] =
    perTumorDnaSampleTopMixtures
      .filter(pair => pair._2.keys.toSet != Set(germlineAlleles._1, germlineAlleles._2))
      .keys
      .toVector

  /** Are we making a somatic call here? */
  val isSomaticCall = !isGermlineCall && tumorDnaSampleIndicesTriggered.nonEmpty

  /** Are we making a germline or somatic call? */
  val isCall = isGermlineCall || isSomaticCall

  /**
   * Names of filters that are causing this potential call to be considered filtered.
   *
   * If this function returns an empty set, then the call is not filtered. That could be because it is a valid passing
   * variant (in which isCalled will be true) or because it was force called and none of the filters happened to fail.
   *
   * Some filters apply to individual samples (e.g. strand bias), whereas others apply to all samples for an allele
   * at a site (e.g. insufficient evidence in normal). The former are implemented in SampleAlleleEvidenceAnnotation,
   * and the latter are in CallsAtSiteAnnotation.
   *
   * The handling for the first kind of filter (per-sample filters) is a bit subtle here, because we do not care about
   * filters that are failing on some samples when, even if those samples were ignored, a call would still be triggered.
   * For example, if two tumors are triggering a somatic variant call and one of them fails the strand bias filter,
   * we do *not* want to treat this call as filtered. If however both of those samples fail the strand bias filter, or
   * even if they both fail different filters, then we do want to consider this call as filtered. A consequence of this
   * is that if isCalled is false, then this function will never return any individual sample filters.
   *
   */
  def failingFilterNames: Set[String] = {
    assume(annotations.isDefined, "Must annotate before calling this method")
    val triggeringEvidences =
      if (isGermlineCall)
        Seq(normalDNAPooledEvidence)
      else if (isSomaticCall)
        tumorDnaSampleIndicesTriggered.map(index => allEvidences(index))
      else
      Seq.empty

    val triggerFailingFilters =
      if (triggeringEvidences.forall(_.annotations.get.annotationsFailingFilters.nonEmpty))
        triggeringEvidences
          .flatMap(_.annotations.get.annotationsFailingFilters)
          .map(_.name)
          .toSet
      else
        Set.empty

    triggerFailingFilters ++
      annotations
        .get
        .annotationsFailingFilters
        .map(_.name)
        .toSet
  }

  /** true if this potential call should be considered to be filtered */
  def failsFilters: Boolean = failingFilterNames.nonEmpty

  /**
   * Return a new instance with all available annotations computed.
   */
  def annotate(multiAlleleEvidence: MultiSampleMultiAlleleEvidence,
               multipleStats: MultiplePileupStats): MultiSampleSingleAlleleEvidence = {

    assume(
      multipleStats
        .singleSampleStats
        .forall(_.referenceSequence.length == allele.end - allele.start)
    )

    copy(
      normalDNAPooledEvidence =
        normalDNAPooledEvidence
          .withAnnotations(
            SingleSampleAnnotations(
              multipleStats.normalDNAPooled,
              normalDNAPooledEvidence,
              parameters
            )
          )
          .asInstanceOf[NormalDNASingleSampleSingleAlleleEvidence],

      tumorDNAPooledEvidence =
        tumorDNAPooledEvidence
          .withAnnotations(
            SingleSampleAnnotations(
              multipleStats.tumorlDNAPooled,
              tumorDNAPooledEvidence,
              parameters
            )
          )
          .asInstanceOf[TumorDNASingleSampleSingleAlleleEvidence],

      sampleEvidences =
        inputs
          .items
          .zip(multipleStats.singleSampleStats)
          .zip(sampleEvidences)
          .map {
            case ((input, stats), evidence) =>
              evidence
                .withAnnotations(
                  SingleSampleAnnotations(
                    stats,
                    evidence,
                    parameters
                  )
                )
          },
      annotations =
        Some(
          MultiSampleAnnotations(
            multiAlleleEvidence,
            this,
            multipleStats,
            parameters
          )
        )
    )
  }
}
object MultiSampleSingleAlleleEvidence {

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
    stats: MultiplePileupStats): MultiSampleSingleAlleleEvidence = {

    val sampleEvidences =
      stats
        .inputs
        .items
        .zip(stats.singleSampleStats)
        .map {
          case (input, stats) =>
            (input.tissueType, input.analyte) match {
              case (TissueType.Normal, Analyte.DNA) => NormalDNASingleSampleSingleAlleleEvidence(allele, stats, parameters)
              case (TissueType.Normal, Analyte.RNA) => throw new IllegalArgumentException("Normal RNA not supported")
              case (TissueType.Tumor, Analyte.DNA)  => TumorDNASingleSampleSingleAlleleEvidence(allele, stats, parameters)
              case (TissueType.Tumor, Analyte.RNA)  => TumorRNASingleSampleSingleAlleleEvidence(allele, stats, parameters)
            }
        }

    val normalDNAPooledCharacterization =
      NormalDNASingleSampleSingleAlleleEvidence(
        allele,
        stats.normalDNAPooled,
        parameters
      )

    val tumorDNAPooledCharacterization =
      TumorDNASingleSampleSingleAlleleEvidence(
        allele,
        stats.tumorlDNAPooled,
        parameters
      )

    MultiSampleSingleAlleleEvidence(
      parameters,
      allele,
      stats.inputs,
      normalDNAPooledCharacterization,
      tumorDNAPooledCharacterization,
      sampleEvidences,
      annotations = None
    )
  }
}
