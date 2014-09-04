package org.bdgenomics.guacamole.variants

import org.bdgenomics.formats.avro.{ Contig, Variant }
import org.bdgenomics.guacamole.{ Bases, HasReferenceRegion }

trait ReferenceVariant extends HasReferenceRegion {

  val sampleName: String

  val referenceContig: String

  /** start locus of the variant */
  val start: Long

  /** reference genome base at the start locus */
  val referenceBase: Byte

  /** alternate base in the variant */
  val alternateBase: Seq[Byte]

  val length: Int

  /** Conversion to ADAMVariant */
  def adamVariant = Variant.newBuilder
    .setStart(start)
    .setEnd(end)
    .setReferenceAllele(Bases.baseToString(referenceBase))
    .setAlternateAllele(Bases.basesToString(alternateBase))
    .setContig(Contig.newBuilder.setContigName(referenceContig).build)
    .build

}
