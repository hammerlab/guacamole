package org.hammerlab.guacamole.commands.jointcaller

import org.hammerlab.guacamole.DistributedUtil.PerSample
import org.hammerlab.guacamole.pileup.Pileup
import org.hammerlab.guacamole.reference.ReferenceBroadcast

/**
 * An allele (alt) at a site in the genome. We also keep track of the reference allele (ref) at this site.
 *
 * Usually alt != ref, but in some cases, such as force calling positions with no variant reads, we can have
 * alt == ref.
 *
 * Indels are supported in the usual VCF style, in which ref.length != alt.length. ref.length is always > 0
 * (e.g. an insertion is represented as A -> ACC) and therefore end > start. The length of the
 * reference allele determines the size of the region.
 *
 * NOTE: We currently evaluate only a single alternate at each site at a time, i.e. the mixtures whose likelihoods we compute
 * are always just a reference and at most one alternate allele. If we extend this to mixtures with multiple alts, we
 * should change this class to contain any nuber of alts.
 *
 * @param referenceContig the contig (chromosome)
 * @param start the position of the allele
 * @param ref reference allele, must be nonempty
 * @param alt alternate allele, may be equal to reference
 */
case class AlleleAtLocus(
    referenceContig: String,
    start: Long,
    ref: String,
    alt: String) {

  assume(ref.nonEmpty)

  lazy val id = "%s:%d-%d %s>%s".format(
    referenceContig,
    start,
    end,
    ref,
    alt)

  /** Zero-based exclusive end site on the reference genome. */
  lazy val end = start + ref.length

  /**
   * Apply a transformation function to the alleles (ref and alt) and also the start and end coordinates, returning
   * a new AlleleAtLocus.
   *
   * This is used when we need to change the number of bases of reference context used, e.g. to change the variant
   * "A>C" to "GA>GC", as part of harmonizing it with other variants of different lengths at the same site.
   *
   * @param alleleTransform transformation function on alleles
   * @param startEndTransform transformation function on (start, end) pairs.
   * @return a new AlleleAtLocus instance
   */
  def transform(alleleTransform: String => String, startEndTransform: (Long, Long) => (Long, Long)): AlleleAtLocus = {
    val newRef = alleleTransform(ref)
    val newAlt = alleleTransform(alt)
    val (newStart, newEnd) = startEndTransform(start, end)
    val result = copy(start = newStart, ref = newRef, alt = newAlt)
    assert(result.end == newEnd)
    result
  }
}
object AlleleAtLocus {

  /**
   * Given one or more pileups, return a sequence of AlleleAtLocus instances giving the possible variant alleles at the
   * *next site* in the pileups.
   *
   * Pileup elements whose *current* position is a variant are ignored here. The alleles returned start at the
   * subsequent reference base and continue as long the bases do not match the reference. This is a pattern used
   * throughout the joint caller: given a pileup at locus X we call variants that start at locus X + 1. This is done to
   * avoid calling variants in the middle of longer variants.
   *
   * @param reference reference genome
   * @param pileups one or more pileups
   * @param anyAlleleMinSupportingReads minimum number of reads in a single sample an allele must have
   * @param anyAlleleMinSupportingPercent minimum percent of reads (i.e. between 0 and 100) an allele must have
   * @param onlyStandardBases only include alleles made entirely of standard bases (no N's)
   * @return the alleles sequenced at this site
   */
  def variantAlleles(reference: ReferenceBroadcast,
                     pileups: PerSample[Pileup],
                     anyAlleleMinSupportingReads: Int,
                     anyAlleleMinSupportingPercent: Double,
                     onlyStandardBases: Boolean = true): Seq[AlleleAtLocus] = {

    assume(pileups.forall(_.locus == pileups.head.locus))
    assume(pileups.forall(_.referenceName == pileups.head.referenceName))
    val contig = pileups.head.referenceName
    val variantStart = pileups.head.locus + 1
    val contigRefSequence = reference.getContig(contig)
    pileups.flatMap(pileup => {
      val requiredReads = math.max(
        anyAlleleMinSupportingReads,
        pileup.elements.size * anyAlleleMinSupportingPercent / 100.0)
      val rawSubsequences = ReadSubsequence.nextAlts(pileup.elements, contigRefSequence)
      val subsequences = if (onlyStandardBases) rawSubsequences.filter(_.sequenceIsAllStandardBases) else rawSubsequences
      subsequences
        .groupBy(_.sequence)
        .filter(_._2.size >= requiredReads)
        .map(pair => {
          val exemplarSubsequence = pair._2.head
          AlleleAtLocus(
            contig, variantStart, exemplarSubsequence.refSequence(contigRefSequence), exemplarSubsequence.sequence)
        }).toSeq
    }).distinct
  }
}