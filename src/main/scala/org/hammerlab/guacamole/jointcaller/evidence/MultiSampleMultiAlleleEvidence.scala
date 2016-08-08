package org.hammerlab.guacamole.jointcaller.evidence

import org.hammerlab.guacamole.jointcaller.pileup_summarization.{MultiplePileupStats, PileupStats}
import org.hammerlab.guacamole.jointcaller.{AlleleAtLocus, InputCollection, Parameters}
import org.hammerlab.guacamole.pileup.Pileup
import org.hammerlab.guacamole.readsets.PerSample
import org.hammerlab.guacamole.reference.Locus
import org.hammerlab.guacamole.reference.{ContigName, ReferenceBroadcast, ReferenceRegion}
import org.hammerlab.guacamole.util.Bases

/**
 * A grouping of AlleleEvidenceAcrossSamples instances (one for each allele) at the same site.
 *
 * Currently not much is done here, and we just write out VCF entries for all the called alleles separately. Later
 * we may want to use this as a place to decide which if any of a number of alleles called at the same site should be
 * written out.
 *
 */
case class MultiSampleMultiAlleleEvidence(contigName: ContigName,
                                          start: Locus,
                                          singleAlleleEvidences: Seq[MultiSampleSingleAlleleEvidence])
    extends ReferenceRegion {

  assume(singleAlleleEvidences.forall(_.allele.contigName == contigName))
  assume(singleAlleleEvidences.forall(_.allele.start == start))

  val end: Locus =
    if (singleAlleleEvidences.isEmpty)
      start
    else
      singleAlleleEvidences.map(_.allele.end).max

  /**
   * If we are going to consider only one allele at this site, pick the best one.
   *
   * TODO: this should probably do something more sophisticated.
   */
  def bestAllele(): MultiSampleSingleAlleleEvidence = {
    // We rank germline calls first, then somatic calls, then break ties with the sum of the best posteriors.
    singleAlleleEvidences.sortBy(evidence => {
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
  def onlyPassingFilters(): MultiSampleMultiAlleleEvidence = {
    copy(singleAlleleEvidences = singleAlleleEvidences.filter(!_.failsFilters))
  }
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

    val contig = normalPileups.head.contigName
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
        val referenceSequence = filteredPileups.head.contigSequence.slice(pair._1, pair._2)
        val stats = filteredPileups.map(pileup => PileupStats(pileup.elements, refSequence = referenceSequence))
        pair -> MultiplePileupStats(inputs, stats)
      }).toMap

    // Create a MultiSampleSingleAlleleEvidence for each alternate allele.
    val allEvidences = possibleAlleles.map(allele => {
      MultiSampleSingleAlleleEvidence(
        parameters,
        allele,
        multiplePileupStatsPerPossibleAlleleLocus((allele.start.toInt, allele.end.toInt)))
    })

    val evidencesCalled = allEvidences.filter(_.isCall)
    val evidences = if (evidencesCalled.nonEmpty) evidencesCalled else allEvidences.take(1)

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

    // Create a MultiSampleMultiAlleleEvidence to group all the alleles and their evidence.
    val calls = MultiSampleMultiAlleleEvidence(
      contigName = evidences.head.allele.contigName,
      start = evidences.head.allele.start,
      singleAlleleEvidences = evidences)

    // Run annotations (e.g. filters).
    val annotatedEvidences = evidences.map(evidence => {
      val allele = evidence.allele
      evidence.annotate(
        calls,
        multiplePileupStatsPerPossibleAlleleLocus((allele.start.toInt, allele.end.toInt)))
    })
    val annotatedCalls = calls.copy(singleAlleleEvidences = annotatedEvidences)

    // If we are force calling this site or are including filtered calls, then we want to return all alleles resulting
    // in calls. Otherwise, we return only the alleles that pass all the filters.
    val passingCalls = if (forceCall || includeFiltered)
      annotatedCalls
    else
      annotatedCalls.onlyPassingFilters()

    // If we have no calls at this point, we aren't calling this site, so return None. Otherwise return the calls.
    if (passingCalls.singleAlleleEvidences.isEmpty)
      None
    else
      Some(passingCalls)
  }
}
