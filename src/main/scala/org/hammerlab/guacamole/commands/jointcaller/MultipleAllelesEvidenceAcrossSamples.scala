package org.hammerlab.guacamole.commands.jointcaller

import org.hammerlab.guacamole.HasReferenceRegion

/**
 * A grouping of AlleleEvidenceAcrossSamples instances at the same locus.
 *
 * Currently not much is done here, and we just write out VCF entries for all the called alleles separately. Later
 * we may want to use this as a place to decide which if any of a number of alleles called at the same site should be
 * written out.
 *
 */
case class MultipleAllelesEvidenceAcrossSamples(alleleEvidences: Seq[AlleleEvidenceAcrossSamples])
    extends HasReferenceRegion {

  assume(alleleEvidences.nonEmpty)

  val referenceContig = alleleEvidences.head.allele.referenceContig
  val start = alleleEvidences.map(_.allele.start).min
  val end = alleleEvidences.map(_.allele.end).max

  assume(alleleEvidences.forall(_.allele.referenceContig == referenceContig))

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
        evidence.pooledGermlinePosteriors.maxBy(_._2)._2 +
        evidence.perTumorSampleSomaticPosteriors.values.map(x => x.maxBy(_._2)._2).max)
    }).last
  }

  /**
   * Return a new instance containing only the best allele in the collection.
   */
  def onlyBest(): MultipleAllelesEvidenceAcrossSamples = MultipleAllelesEvidenceAcrossSamples(Seq(bestAllele))
}