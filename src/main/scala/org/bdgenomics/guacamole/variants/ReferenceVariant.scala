package org.bdgenomics.guacamole.variants

import org.bdgenomics.formats.avro.{ Contig, Variant }
import org.bdgenomics.guacamole.pileup.Allele
import org.bdgenomics.guacamole.{ Bases, HasReferenceRegion }

/**
 * Base properties of a genomic change in a sequence sample from a reference genome
 */
trait ReferenceVariant extends HasReferenceRegion {

  val sampleName: String

  val referenceContig: String

  /** start locus of the variant */
  val start: Long

  /** reference and sequenced bases for this variant */
  val allele: Allele

  val length: Int

  /** Conversion to ADAMVariant */
  def adamVariant = Variant.newBuilder
    .setStart(start)
    .setEnd(end)
    .setReferenceAllele(Bases.basesToString(allele.refBases))
    .setAlternateAllele(Bases.basesToString(allele.altBases))
    .setContig(Contig.newBuilder.setContigName(referenceContig).build)
    .build

}
