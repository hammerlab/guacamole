package org.hammerlab.guacamole.variants

import org.bdgenomics.formats.avro.{DatabaseVariantAnnotation, Variant, Genotype => BDGGenotype}
import org.hammerlab.guacamole.readsets.SampleName
import org.hammerlab.guacamole.reference.ReferenceRegion
import org.hammerlab.guacamole.util.Bases.basesToString

/**
 * Base properties of a genomic change in a sequence sample from a reference genome
 */
trait ReferenceVariant extends ReferenceRegion {

  def sampleName: SampleName

  /** reference and sequenced bases for this variant */
  def allele: Allele

  /** Conversion to ADAMVariant */
  def bdgVariant: Variant =
    Variant
      .newBuilder
      .setStart(start)
      .setEnd(end)
      .setReferenceAllele(basesToString(allele.refBases))
      .setAlternateAllele(basesToString(allele.altBases))
      .setContigName(contigName)
      .build

  def rsID: Option[Int]

  def bdgVariantDatabase: DatabaseVariantAnnotation = {
    val builder = DatabaseVariantAnnotation.newBuilder
    rsID.foreach(builder.setDbSnpId(_))
    builder.build
  }

  def toBDGGenotype: BDGGenotype
}
