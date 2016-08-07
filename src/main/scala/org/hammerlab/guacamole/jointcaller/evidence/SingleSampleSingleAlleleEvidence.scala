package org.hammerlab.guacamole.jointcaller.evidence

import org.hammerlab.guacamole.jointcaller.AlleleAtLocus
import org.hammerlab.guacamole.jointcaller.annotation.SingleSampleAnnotations

/**
 * Summary of evidence for an allele at a site in a single sample. This trait is implemented by tissue- and
 * analyte-specific classes.
 *
 * The classes implementing this should be serializable. They should gather all the per-sample information needed to
 * call or not call a variant. They do not themselves call variants, however, as that requires looking across multiple
 * samples.
 */
trait SingleSampleSingleAlleleEvidence {

  /** The allele under consideration */
  val allele: AlleleAtLocus

  /** extra information, including filters, we compute about a potential call */
  val annotations: Option[SingleSampleAnnotations]

  /** return a new instance including the given annotations */
  def withAnnotations(annotations: SingleSampleAnnotations): SingleSampleSingleAlleleEvidence
}
