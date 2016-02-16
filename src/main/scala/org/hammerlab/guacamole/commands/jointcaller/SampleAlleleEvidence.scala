package org.hammerlab.guacamole.commands.jointcaller

/**
 * Summary of evidence for an allele at a site in a single sample. This trait is implemented by tissue- and
 * analyte-specific classes.
 *
 * The classes implementing this should be serializable. They should gather all the per-sample information needed to
 * call or not call a variant. They do not themselves call variants, however, as that requires looking across multiple
 * samples.
 */
trait SampleAlleleEvidence {

  /** The allele under consideration */
  val allele: AlleleAtLocus

  /**
   * Apply a transformation function to the alleles. See AlleleAtLocus.transformAlleles for details.
   */
  def transformAlleles(alleleTransform: String => String,
                       startEndTransform: (Long, Long) => (Long, Long)): SampleAlleleEvidence
}