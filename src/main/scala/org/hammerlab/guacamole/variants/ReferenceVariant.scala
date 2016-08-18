package org.hammerlab.guacamole.variants

import org.bdgenomics.formats.avro.{Contig, DatabaseVariantAnnotation, Variant}
import org.hammerlab.guacamole.readsets.SampleName
import org.hammerlab.guacamole.reference.ReferenceRegion
import org.hammerlab.guacamole.util.Bases

/**
 * Base properties of a genomic change in a sequence sample from a reference genome
 */
trait ReferenceVariant extends ReferenceRegion {

  def sampleName: SampleName

  /** reference and sequenced bases for this variant */
  def allele: Allele

  /** Conversion to ADAMVariant */
  def adamVariant =
    Variant.newBuilder
      .setStart(start)
      .setEnd(end)
      .setReferenceAllele(Bases.basesToString(allele.refBases))
      .setAlternateAllele(Bases.basesToString(allele.altBases))
      .setContig(Contig.newBuilder.setContigName(contigName).build)
      .build

  def rsID: Option[Int]

  def adamVariantDatabase = {
    val builder = DatabaseVariantAnnotation.newBuilder
    rsID.foreach(builder.setDbSnpId(_))
    builder.build
  }
}
