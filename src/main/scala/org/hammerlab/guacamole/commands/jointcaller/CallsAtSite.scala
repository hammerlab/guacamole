package org.hammerlab.guacamole.commands.jointcaller

import org.hammerlab.guacamole.DistributedUtil._
import org.hammerlab.guacamole.commands.jointcaller.CallsAtSiteAnnotation.NamedAnnotations
import org.hammerlab.guacamole.pileup.Pileup
import org.hammerlab.guacamole.reference.ReferenceBroadcast
import org.hammerlab.guacamole._

/**
 * A grouping of AlleleEvidenceAcrossSamples instances at the same locus.
 *
 * Currently not much is done here, and we just write out VCF entries for all the called alleles separately. Later
 * we may want to use this as a place to decide which if any of a number of alleles called at the same site should be
 * written out.
 *
 */
case class CallsAtSite(referenceContig: String,
                       start: Long,
                        alleleEvidences: Seq[AlleleEvidenceAcrossSamples],
                        annotations: NamedAnnotations = CallsAtSiteAnnotation.emptyAnnotations)
    extends HasReferenceRegion {

  assume(alleleEvidences.forall(_.allele.referenceContig == referenceContig))
  assume(alleleEvidences.forall(_.allele.start == start))

  val end: Long = if (alleleEvidences.isEmpty) start else alleleEvidences.map(_.allele.end).max

  /**
    * If we are going to consider only one allele at this site, pick the best one.
    *
    * TODO: this should probably do something more sophisticated.
    */
  def bestAllele(): AlleleEvidenceAcrossSamples = {
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
  def onlyPassingFilters(): CallsAtSite = copy(alleleEvidences = alleleEvidences.filter(!_.failsFilters))

  def withAnnotations(annotations: NamedAnnotations): CallsAtSite = copy(
    alleleEvidences = alleleEvidences.map(evidence => evidence.copy(annotations = evidence.annotations ++ annotations))
  )

}
object CallsAtSite {
  def make(
             pileups: PerSample[Pileup],
             inputs: InputCollection,
             parameters: Parameters,
             reference: ReferenceBroadcast,
             forceCallLoci: LociSet = LociSet.empty,
             onlySomatic: Boolean = false,
             includeFiltered: Boolean = false): Option[CallsAtSite] = {

    // We ignore clipped reads. Clipped reads include introns (cigar operator N) in RNA-seq.
    val filteredPileups: Vector[Pileup] = pileups.map(
      pileup => pileup.copy(elements = pileup.elements.filter(!_.isClipped))).toVector
    val normalPileups = inputs.normalDNA.map(input => filteredPileups(input.index))
    val tumorDNAPileups = inputs.tumorDNA.map(input => filteredPileups(input.index))
    val forceCall = forceCallLoci.onContig(filteredPileups(0).referenceName).contains(filteredPileups(0).locus + 1)

    val contig = normalPileups.head.referenceName
    val locus = normalPileups.head.locus

    // We only call variants at a site if the reference base is a standard base (i.e. not N).
    if (!Bases.isStandardBase(reference.getReferenceBase(contig, locus.toInt + 1))) {
      return None
    }

    val possibleAlleles = AlleleAtLocus.variantAlleles(
      (inputs.normalDNA ++ inputs.tumorDNA).map(input => filteredPileups(input.index)),
      anyAlleleMinSupportingReads = parameters.anyAlleleMinSupportingReads,
      anyAlleleMinSupportingPercent = parameters.anyAlleleMinSupportingPercent,
      maxAlleles = Some(parameters.maxAllelesPerSite),
      atLeastOneAllele = forceCall, // if force calling this site, always get at least one allele
      onlyStandardBases = true)

    if (possibleAlleles.isEmpty) {
      assert(!forceCall)
      return None
    }

    val multiplePileupStatsPerPossibleAlleleLocus = possibleAlleles
      .map(allele => (allele.start.toInt, allele.end.toInt))
      .distinct
      .map(pair => {
        val referenceSequence = filteredPileups.head.referenceContigSequence.slice(pair._1, pair._2)
        val stats = filteredPileups.map(pileup => PileupStats(pileup.elements, refSequence = referenceSequence))
        pair -> MultiplePileupStats(inputs, stats)
      }).toMap

    val evidences = possibleAlleles.map(allele => {
      AlleleEvidenceAcrossSamples(
        parameters,
        allele,
        multiplePileupStatsPerPossibleAlleleLocus((allele.start.toInt, allele.end.toInt)))
    })

    if (!forceCall) {
      if (!evidences.exists(_.isCall)) {
        return None
      }
      if (onlySomatic && evidences.exists(_.isGermlineCall)) {
        return None
      }
    }

    val annotatedEvidences = evidences.map(evidence => {
      val allele = evidence.allele
      evidence.computeAllAnnotations(multiplePileupStatsPerPossibleAlleleLocus((allele.start.toInt, allele.end.toInt)))
    })
    val calls = CallsAtSite(
      referenceContig = annotatedEvidences.head.allele.referenceContig,
      start = annotatedEvidences.head.allele.start,
      alleleEvidences = annotatedEvidences)

    val annotatedCallsAtSite = calls.copy(annotations = CallsAtSiteAnnotation.makeAnnotations(calls, parameters))

    val passingCalls = if (forceCall || includeFiltered)
      annotatedCallsAtSite
    else
      annotatedCallsAtSite.onlyPassingFilters()

    if (passingCalls.alleleEvidences.isEmpty)
      None
    else
      Some(passingCalls)
  }
}