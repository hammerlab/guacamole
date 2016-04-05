package org.hammerlab.guacamole.commands.jointcaller.evidence

import org.hammerlab.guacamole.DistributedUtil._
import org.hammerlab.guacamole._
import org.hammerlab.guacamole.commands.jointcaller.annotation.MultiSampleMultiAlleleAnnotation
import org.hammerlab.guacamole.commands.jointcaller.annotation.MultiSampleMultiAlleleAnnotation.NamedAnnotations
import org.hammerlab.guacamole.commands.jointcaller.pileup_summarization.{ MultiplePileupStats, PileupStats }
import org.hammerlab.guacamole.commands.jointcaller.{ AlleleAtLocus, InputCollection, Parameters }
import org.hammerlab.guacamole.pileup.Pileup
import org.hammerlab.guacamole.reference.ReferenceBroadcast

/**
 * A grouping of AlleleEvidenceAcrossSamples instances (one for each allele) at the same site.
 *
 * Currently not much is done here, and we just write out VCF entries for all the called alleles separately. Later
 * we may want to use this as a place to decide which if any of a number of alleles called at the same site should be
 * written out.
 *
 */
case class MultiSampleMultiAlleleEvidence(referenceContig: String,
                                          start: Long,
                                          alleleEvidences: Seq[MultiSampleSingleAlleleEvidence],
                                          annotations: NamedAnnotations = MultiSampleMultiAlleleAnnotation.emptyAnnotations)
    extends HasReferenceRegion {

  assume(alleleEvidences.forall(_.allele.referenceContig == referenceContig))
  assume(alleleEvidences.forall(_.allele.start == start))

  val end: Long = if (alleleEvidences.isEmpty) start else alleleEvidences.map(_.allele.end).max

  /**
   * If we are going to consider only one allele at this site, pick the best one.
   *
   * TODO: this should probably do something more sophisticated.
   */
  def bestAllele(): MultiSampleSingleAlleleEvidence = {
    // We rank germline calls first, then somatic calls, then break ties with the sum of the best posteriors.
    alleleEvidences.sortBy(evidence => {
      (evidence.isGermlineCall,
        evidence.isSomaticCall,
        if (evidence.isGermlineCall)
          evidence.pooledGermlinePosteriors.maxBy(_._2)._2
        else
          evidence.perTumorDnaSampleSomaticPosteriors.values.map(x => x.maxBy(_._2)._2).max)
    }).last
  }

  /**
   * Return a new instance containing only the calls that pass filters.
   */
  def onlyPassingFilters(): MultiSampleMultiAlleleEvidence = copy(alleleEvidences = alleleEvidences.filter(!_.failsFilters))

  /**
   * Return a new instance with the given annotations.
   *
   * The new annotations are copied into all of the individual AlleleEvidenceAcrossSamples instances.
   */
  def withAnnotations(annotations: NamedAnnotations): MultiSampleMultiAlleleEvidence = copy(
    alleleEvidences = alleleEvidences.map(evidence => evidence.copy(annotations = evidence.annotations ++ annotations))
  )
}
object MultiSampleMultiAlleleEvidence {

  /**
   * Maybe make a AllelesAndEvidenceAtSite instance given pileups for all samples at a particular site.
   *
   * Returns None if there is no call to be made at the site (e.g. if we're not force calling the site and there is
   * insufficient evidence for a call).
   *
   * @param pileups pileup instances such that pileups(i) corresponds to sample input i
   * @param inputs sample inputs description
   * @param parameters variant calling parameters
   * @param reference genome reference
   * @param forceCall return an instance for at this site even if there is insufficient evidence for a call
   * @param onlySomatic only return somatic calls
   * @param includeFiltered return calls even if they are filtered
   * @return
   */
  def make(
    pileups: PerSample[Pileup],
    inputs: InputCollection,
    parameters: Parameters,
    reference: ReferenceBroadcast,
    forceCall: Boolean,
    onlySomatic: Boolean = false,
    includeFiltered: Boolean = false): Option[MultiSampleMultiAlleleEvidence] = {

    // We ignore clipped reads. Clipped reads include introns (cigar operator N) in RNA-seq.
    val filteredPileups: Vector[Pileup] = pileups.map(
      pileup => pileup.copy(elements = pileup.elements.filter(!_.isClipped))).toVector
    val normalPileups = inputs.normalDNA.map(input => filteredPileups(input.index))
    val tumorDNAPileups = inputs.tumorDNA.map(input => filteredPileups(input.index))

    val contig = normalPileups.head.referenceName
    val locus = normalPileups.head.locus

    // We only call variants at a site if the reference base is a standard base (i.e. not N).
    if (!Bases.isStandardBase(reference.getReferenceBase(contig, locus.toInt + 1))) {
      return None
    }

    // Collect possible alternate alleles to consider. These are any consecutive non-matching bases in the reads
    // satisfying fairly permissive evidence criteria (typically, >=2 reads supporting a variant).
    // This will return an empty seq if there are no alternate alleles sequenced. If we are force calling this site,
    // we require that we always get back at least one allele (which will be N if there are no alternate alleles
    // at all).
    val possibleAlleles = AlleleAtLocus.variantAlleles(
      (inputs.normalDNA ++ inputs.tumorDNA).map(input => filteredPileups(input.index)),
      anyAlleleMinSupportingReads = parameters.anyAlleleMinSupportingReads,
      anyAlleleMinSupportingPercent = parameters.anyAlleleMinSupportingPercent,
      maxAlleles = Some(parameters.maxAllelesPerSite),
      atLeastOneAllele = forceCall, // if force calling this site, always get at least one allele
      onlyStandardBases = true)

    // If we have no possible alternate alleles, don't call anything at this site.
    if (possibleAlleles.isEmpty) {
      assert(!forceCall)
      return None
    }

    // We need a MultiplePileupStats instance for each allele. Since PileupStats (and MultiplePileupStats) don't depend
    // on the actual alternate allele, but only its start and end positions, as an optimization we make one
    // MultiplePileupStats per allele (start, end) position.
    val multiplePileupStatsPerPossibleAlleleLocus = possibleAlleles
      .map(allele => (allele.start.toInt, allele.end.toInt))
      .distinct
      .map(pair => {
        val referenceSequence = filteredPileups.head.referenceContigSequence.slice(pair._1, pair._2)
        val stats = filteredPileups.map(pileup => PileupStats(pileup.elements, refSequence = referenceSequence))
        pair -> MultiplePileupStats(inputs, stats)
      }).toMap

    // Create a MultiSampleSingleAlleleEvidence for each alternate allele.
    val evidences = possibleAlleles.map(allele => {
      MultiSampleSingleAlleleEvidence(
        parameters,
        allele,
        multiplePileupStatsPerPossibleAlleleLocus((allele.start.toInt, allele.end.toInt)))
    })

    // Unless we're force calling, we want to skip this call if no alleles have a likelihood favoring a variant call.
    // We also want to skip this call if we're only calling somatic variants and any of the alleles considered generate
    // a germline call.
    if (!forceCall) {
      if (!evidences.exists(_.isCall)) {
        return None
      }
      if (onlySomatic && evidences.exists(_.isGermlineCall)) {
        return None
      }
    }

    // If we've made it to this point, then it's now time to run annotations (e.g. filters).
    // First, annotate each individual MultiSampleSingleAlleleEvidence instance.
    val annotatedEvidences = evidences.map(evidence => {
      val allele = evidence.allele
      evidence.computeAllAnnotations(multiplePileupStatsPerPossibleAlleleLocus((allele.start.toInt, allele.end.toInt)))
    })

    // Create a MultiSampleMultiAlleleEvidence to group all the alleles and their evidence.
    val calls = MultiSampleMultiAlleleEvidence(
      referenceContig = annotatedEvidences.head.allele.referenceContig,
      start = annotatedEvidences.head.allele.start,
      alleleEvidences = annotatedEvidences)

    // Now annotate it.
    val annotatedCallsAtSite = calls.copy(annotations = MultiSampleMultiAlleleAnnotation.makeAnnotations(calls, parameters))

    // If we are force calling this site or are including filtered calls, then we want to return all alleles resulting
    // in calls. Otherwise, we return only the alleles that pass all the filters.
    val passingCalls = if (forceCall || includeFiltered)
      annotatedCallsAtSite
    else
      annotatedCallsAtSite.onlyPassingFilters()

    // If we have no calls at this point, we aren't calling this site, so return None. Otherwise return the calls.
    if (passingCalls.alleleEvidences.isEmpty)
      None
    else
      Some(passingCalls)
  }
}