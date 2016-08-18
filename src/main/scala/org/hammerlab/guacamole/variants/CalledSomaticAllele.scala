package org.hammerlab.guacamole.variants

import org.bdgenomics.adam.util.PhredUtils
import org.hammerlab.guacamole.readsets.SampleName
import org.hammerlab.guacamole.reference.{ContigName, Locus, NumLoci}

/**
 *
 * A variant that exists in a tumor sample, but not in the normal sample; includes supporting read statistics from both samples
 *
 * @param sampleName sample the variant was called on
 * @param contigName chromosome or genome contig of the variant
 * @param start start position of the variant (0-based)
 * @param allele reference and sequence bases of this variant
 * @param somaticLogOdds log odds-ratio of the variant in the tumor compared to the normal sample
 * @param tumorVariantEvidence supporting statistics for the variant in the tumor sample
 * @param normalReferenceEvidence supporting statistics for the reference in the normal sample
 * @param rsID   identifier for the variant if it is in dbSNP
 * @param length length of the variant
 */
case class CalledSomaticAllele(sampleName: SampleName,
                               contigName: ContigName,
                               start: Locus,
                               allele: Allele,
                               somaticLogOdds: Double,
                               tumorVariantEvidence: AlleleEvidence,
                               normalReferenceEvidence: AlleleEvidence,
                               rsID: Option[Int] = None,
                               override val length: NumLoci = 1) extends ReferenceVariant {
  val end: Locus = start + 1L

  // P ( variant in tumor AND no variant in normal) = P(variant in tumor) * P(reference in normal)
  lazy val phredScaledSomaticLikelihood =
    PhredUtils.successProbabilityToPhred(tumorVariantEvidence.likelihood * normalReferenceEvidence.likelihood - 1e-10)
}
