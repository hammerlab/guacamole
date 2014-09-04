package org.bdgenomics.guacamole.variants

import org.bdgenomics.formats.avro.{Contig, Variant}
import org.bdgenomics.guacamole.{Bases, HasReferenceRegion}


trait Variant extends HasReferenceRegion {

  val sampleName: String
  val referenceContig: String
  val start: Long
  val referenceBase: Byte
  val alternateBase: Seq[Byte]
  val length: Int

  def adamVariant = Variant.newBuilder
    .setStart(start)
    .setEnd(end)
    .setReferenceAllele(Bases.baseToString(referenceBase))
    .setAlternateAllele(Bases.basesToString(alternateBase))
    .setContig(Contig.newBuilder.setContigName(referenceContig).build)
    .build

}
