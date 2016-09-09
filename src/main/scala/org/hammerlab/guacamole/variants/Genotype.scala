package org.hammerlab.guacamole.variants

/**
 * A Genotype is a map of alleles to their allele frequency
 *
 * A Genotype is for a particular reference locus. Each allele gives the base(s) present on a chromosome at that
 * locus.
 *
 */
case class Genotype(alleleMixture: Map[Allele, Double]) {

  // The allele fractions should sum up to approximately one
  assume(alleleMixture.values.sum == 1, s"Allele should sum to 1, but sum to ${alleleMixture.values.sum}")

  val alleles = alleleMixture.keySet

  lazy val getNonReferenceAlleles: Set[Allele] = {
    alleles.filter(_.isVariant)
  }

  /**
   * Counts alleles in this genotype that are not the same as the specified reference allele.
   *
   * @return Count of non reference alleles
   */
  lazy val numberOfVariantAlleles: Int = getNonReferenceAlleles.size

  /**
   * Returns whether this genotype contains any non-reference alleles for a given reference sequence.
   *
   * @return True if at least one allele is not the reference
   */
  lazy val hasVariantAllele: Boolean = (numberOfVariantAlleles > 0)

  override def toString: String = "Genotype(%s)".format(alleles.map(_.toString).mkString(","))
}
