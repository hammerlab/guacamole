package org.bdgenomics.guacamole.variants

import org.bdgenomics.formats.avro.{ Contig, Variant }
import org.bdgenomics.guacamole.{ Bases, HasReferenceRegion }

/**
 * Base properties of a genomic change in a sequence sample from a reference genome
 */
trait ReferenceVariant extends HasReferenceRegion {

  val sampleName: String

  val referenceContig: String

  /** start locus of the variant */
  val start: Long

  /** reference genome base at the start locus */
  val referenceBases: Seq[Byte]

  /** alternate base in the variant */
  val alternateBases: Seq[Byte]

  val length: Int

  /** Conversion to ADAMVariant */
  def adamVariant = Variant.newBuilder
    .setStart(start)
    .setEnd(end)
    .setReferenceAllele(Bases.basesToString(referenceBases))
    .setAlternateAllele(Bases.basesToString(alternateBases))
    .setContig(Contig.newBuilder.setContigName(referenceContig).build)
    .build

}
