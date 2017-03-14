package org.hammerlab.guacamole.variants

import org.bdgenomics.formats.avro.GenotypeAllele.{ ALT, REF }
import org.bdgenomics.formats.avro.{ Genotype ⇒ BDGGenotype }
import org.hammerlab.genomics.readsets.SampleName
import org.hammerlab.genomics.reference.{ ContigName, Locus, NumLoci }
import org.hammerlab.guacamole.util.PhredUtils.successProbabilityToPhred
import org.hammerlab.guacamole.variants.CalledSomaticAllele.alleles

import scala.collection.JavaConversions.seqAsJavaList

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
                               override val length: NumLoci = NumLoci(1))
  extends ReferenceVariant {
  val end: Locus = start + length

  // P ( variant in tumor AND no variant in normal) = P(variant in tumor) * P(reference in normal)
  lazy val phredScaledSomaticLikelihood =
    successProbabilityToPhred(
      tumorVariantEvidence.probability * normalReferenceEvidence.probability
    )

  def toBDGGenotype: BDGGenotype =
    BDGGenotype
      .newBuilder
      .setAlleles(alleles)
      .setSampleId(sampleName)
      .setContigName(contigName.name)
      .setStart(start.locus)
      .setEnd(end.locus)
      .setGenotypeQuality(phredScaledSomaticLikelihood)
      .setReadDepth(tumorVariantEvidence.readDepth)
      .setExpectedAlleleDosage(
        tumorVariantEvidence.alleleReadDepth.toFloat / tumorVariantEvidence.readDepth
      )
      .setReferenceReadDepth(tumorVariantEvidence.readDepth - tumorVariantEvidence.alleleReadDepth)
      .setAlternateReadDepth(tumorVariantEvidence.alleleReadDepth)
      .setVariant(bdgVariant)
      .build
}

object CalledSomaticAllele {
  val alleles = seqAsJavaList(Seq(REF, ALT))
}
