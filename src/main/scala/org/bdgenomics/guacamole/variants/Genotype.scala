package org.bdgenomics.guacamole.variants

import org.bdgenomics.adam.util.PhredUtils
import org.bdgenomics.formats.avro.GenotypeAllele
import org.bdgenomics.guacamole.Bases.BasesOrdering
import org.bdgenomics.guacamole.pileup.{ Pileup, PileupElement }

/**
 * A Genotype is a sequence of alleles of length equal to the ploidy of the organism.
 *
 * A Genotype is for a particular reference locus. Each allele gives the base(s) present on a chromosome at that
 * locus.
 *
 * For example, the possible single-base diploid genotypes are Seq('A', 'A'), Seq('A', 'T') ... Seq('T', 'T').
 * Alleles can also be multiple bases as well, e.g. Seq("AAA", "T")
 *
 */
case class Genotype(alleles: Allele*) {
  /**
   * The ploidy of the organism is the number of alleles in the genotype.
   */
  val ploidy = alleles.size

  lazy val uniqueAllelesCount = alleles.toSet.size

  lazy val getNonReferenceAlleles: Seq[Allele] = {
    alleles.filter(_.isVariant)
  }

  def computeElementLikelihood(element: PileupElement, includeAlignmentLikelihood: Boolean = false): Double = {
    val baseCallProbability = PhredUtils.phredToSuccessProbability(element.qualityScore)
    val successProbability = if (includeAlignmentLikelihood) {
      baseCallProbability * element.read.alignmentLikelihood
    } else {
      baseCallProbability
    }

    alleles.map(allele =>
      if (allele == element.allele)
        successProbability
      else
        (1 - successProbability)
    ).sum
  }

  /**
   * Counts alleles in this genotype that are not the same as the specified reference allele.
   *
   * @return Count of non reference alleles
   */
  lazy val numberOfVariants: Int = getNonReferenceAlleles.size

  def likelihoodOfReads(elements: Seq[PileupElement], includeAlignmentLikelihood: Boolean = false) = {
    val depth = elements.size
    val elementLikelihoods =
      // TODO(ryan): we could memoize a lot of computation here by keeping around a count of each genotype's
      // frequency in the Pileup, which we've already had an opportunity to compute and save.
      elements.map(
        computeElementLikelihood(_, includeAlignmentLikelihood)
      )

    elementLikelihoods.product / math.pow(ploidy, depth)
  }

  def logLikelihoodOfReads(elements: Seq[PileupElement], includeAlignmentLikelihood: Boolean = false): Double = {
    val depth = elements.size
    val unnormalizedLikelihood =
      elements
        .map(el => math.log(computeElementLikelihood(el, includeAlignmentLikelihood)))
        .sum
    unnormalizedLikelihood - depth * math.log(ploidy)
  }

  /**
   * Returns whether this genotype contains any non-reference alleles for a given reference sequence.
   *
   * @return True if at least one allele is not the reference
   */
  lazy val isVariant: Boolean = (numberOfVariants > 0)

  /**
   * Transform the alleles in this genotype to the ADAM allele enumeration.
   * Classifies alleles as Reference or Alternate.
   *
   * @return Sequence of Genotype which are Ref, Alt or OtherAlt.
   */
  lazy val getGenotypeAlleles: Seq[GenotypeAllele] = {
    assume(ploidy == 2)
    val numVariants = numberOfVariants
    if (numVariants == 0) {
      Seq(GenotypeAllele.Ref, GenotypeAllele.Ref)
    } else if (numVariants > 0 && uniqueAllelesCount == 1) {
      Seq(GenotypeAllele.Alt, GenotypeAllele.Alt)
    } else if (numVariants >= 2 && uniqueAllelesCount > 1) {
      Seq(GenotypeAllele.Alt, GenotypeAllele.OtherAlt)
    } else {
      Seq(GenotypeAllele.Ref, GenotypeAllele.Alt)
    }
  }

  override def toString: String = "Genotype(%s)".format(alleles.map(_.toString).mkString(","))
}

object AlleleOrdering extends Ordering[Allele] {
  override def compare(x: Allele, y: Allele): Int = {
    BasesOrdering.compare(x.refBases, y.refBases) match {
      case 0 => BasesOrdering.compare(x.altBases, y.altBases)
      case x => x
    }
  }
}
