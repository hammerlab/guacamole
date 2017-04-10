package org.hammerlab.guacamole.jointcaller

import org.hammerlab.genomics.bases.Base.N
import org.hammerlab.genomics.bases.Bases
import org.hammerlab.genomics.readsets.PerSample
import org.hammerlab.genomics.reference.{ ContigName, Locus, Region }
import org.hammerlab.guacamole.jointcaller.pileup_summarization.ReadSubsequence.nextAlts
import org.hammerlab.guacamole.pileup.Pileup

import scala.math.max

/**
 * An allele (alt) at a site in the genome. We also keep track of the reference allele (ref) at this site.
 *
 * Usually alt != ref, but in some cases, such as force calling positions with no variant reads, we can have
 * alt == ref.
 *
 * Indels are supported in the usual VCF style, in which ref.length != alt.length. ref.length and alt.length are > 0
 * (e.g. an insertion is represented as A → ACC) and therefore end > start. The length of the
 * reference allele determines the size of the region.
 *
 * NOTE: We currently evaluate only a single alternate at each site at a time, i.e. the mixtures whose likelihoods we compute
 * are always just a reference and at most one alternate allele. If we extend this to mixtures with multiple alts, we
 * should change this class to contain any nuber of alts.
 *
 * @param contigName the contig (chromosome)
 * @param start the position of the allele
 * @param ref reference allele, must be nonempty
 * @param alt alternate allele, may be equal to reference
 */
case class AlleleAtLocus(contigName: ContigName,
                         start: Locus,
                         ref: Bases,
                         alt: Bases)
  extends Region {

  assume(ref.nonEmpty)
  assume(alt.nonEmpty)

  @transient lazy val id = s"${super.toString} $ref>$alt"

  /** Zero-based exclusive end site on the reference genome. */
  @transient lazy val end = start + ref.length

  override def toString: String = id
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
   * @param pileups one or more pileups
   * @param anyAlleleMinSupportingReads minimum number of reads in a single sample an allele must have
   * @param anyAlleleMinSupportingPercent minimum percent of reads (i.e. between 0 and 100) an allele must have
   * @param maxAlleles if not None, return at most this many alleles
   * @param atLeastOneAllele if true, then always return at least one allele (useful for force calling). If no
   *                         alleles meet the minimum number of reads criteria, then the allele with the most
   *                         reads (even though it doesn't meet the threshold) will be returned. If there are
   *                         no alternate alleles at all, then the allele "N" is returned.
   * @param onlyStandardBases only include alleles made entirely of standard bases (no N's)
   * @return the alleles sequenced at this site
   */
  def variantAlleles(pileups: PerSample[Pileup],
                     anyAlleleMinSupportingReads: Int,
                     anyAlleleMinSupportingPercent: Double,
                     maxAlleles: Option[Int] = None,
                     atLeastOneAllele: Boolean = false,
                     onlyStandardBases: Boolean = true): Vector[AlleleAtLocus] = {

    assume(pileups.forall(_.locus == pileups.head.locus))
    assume(pileups.forall(_.contigName == pileups.head.contigName))
    assume(pileups.nonEmpty)

    val contigSequence = pileups.head.contig

    val contig = pileups.head.contigName
    val variantStart = pileups.head.locus.next

    val alleleRequiredReadsActualReads =
      pileups.flatMap {
        pileup ⇒
          val requiredReads =
            max(
              anyAlleleMinSupportingReads,
              pileup.elements.size * anyAlleleMinSupportingPercent / 100.0
            )

          val subsequenceCounts =
            nextAlts(pileup.elements)
              .filter(subsequence ⇒ !onlyStandardBases || subsequence.allStandardBases)
              .groupBy(x ⇒ (x.end, x.sequence))
              .map { case (_, subsequences) ⇒ subsequences.head → subsequences.length }
              .toVector
              .sortBy(-1 * _._2)

          subsequenceCounts.map {
            case (subsequence, count) ⇒
              (
                AlleleAtLocus(
                  contig,
                  variantStart,
                  subsequence.refSequence(contigSequence),
                  subsequence.sequence
                ),
                requiredReads,
                count
              )
          }
      }

    val result =
      alleleRequiredReadsActualReads
        .filter { case (allele, requiredReads, count) ⇒ count >= requiredReads }
        .map(_._1)
        .distinct  // Reduce to distinct alleles
        .toVector

    if (atLeastOneAllele && result.isEmpty) {
      val allelesSortedByTotal =
        alleleRequiredReadsActualReads
          .groupBy(_._1)
          .toVector
          .sortBy(-1 * _._2.map(_._3).sum)
          .map(_._1)

      if (allelesSortedByTotal.nonEmpty)
        allelesSortedByTotal.take(1)
      else
        Vector(
          AlleleAtLocus(
            contig,
            variantStart,
            Bases(contigSequence(variantStart)),
            Bases(N)
          )
        )
    } else if (maxAlleles.isDefined) {
      assume(maxAlleles.get > 0)
      result.take(maxAlleles.get)
    } else
      result
  }
}
