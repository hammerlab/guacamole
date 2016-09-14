package org.hammerlab.guacamole.variants

import org.hammerlab.guacamole.readsets.SampleName
import scala.collection.JavaConversions.seqAsJavaList
import org.hammerlab.guacamole.reference.{ContigName, Locus, NumLoci}
import org.bdgenomics.formats.avro.{ GenotypeAllele, Genotype => BDGGenotype }

/**
 *
 * A variant that exists in the sample; includes supporting read statistics
 *
 * @param sampleName sample the variant was called on
 * @param contigName chromosome or genome contig of the variant
 * @param start start position of the variant (0-based)
 * @param allele allele (ref + seq bases) for this variant
 * @param evidence supporting statistics for the variant
 * @param length length of the variant
 */
case class CalledAllele(sampleName: SampleName,
                        contigName: ContigName,
                        start: Locus,
                        allele: Allele,
                        evidence: AlleleEvidence,
                        rsID: Option[Int] = None,
                        override val length: NumLoci = 1) extends ReferenceVariant {
  val end: Locus = start + 1L

  def toBDGGenotype: BDGGenotype =
    BDGGenotype
      .newBuilder
      .setAlleles(seqAsJavaList(Seq(GenotypeAllele.Ref, GenotypeAllele.Alt)))
      .setSampleId(sampleName)
      .setGenotypeQuality(evidence.phredScaledLikelihood)
      .setReadDepth(evidence.readDepth)
      .setExpectedAlleleDosage(
        evidence.alleleReadDepth.toFloat / evidence.readDepth
      )
      .setReferenceReadDepth(evidence.readDepth - evidence.alleleReadDepth)
      .setAlternateReadDepth(evidence.alleleReadDepth)
      .setVariant(bdgVariant)
      .build
}
