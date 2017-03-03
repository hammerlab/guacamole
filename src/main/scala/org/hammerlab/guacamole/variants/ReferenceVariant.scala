package org.hammerlab.guacamole.variants

import org.bdgenomics.formats.avro.{ Variant, Genotype ⇒ BDGGenotype }
import org.hammerlab.genomics.reference.Region
import org.hammerlab.guacamole.readsets.SampleName

/**
 * Base properties of a genomic change in a sequence sample from a reference genome
 */
trait ReferenceVariant extends Region {

  def sampleName: SampleName

  /** reference and sequenced bases for this variant */
  def allele: Allele

  /** Conversion to ADAMVariant */
  def bdgVariant: Variant =
    Variant
      .newBuilder
      .setStart(start.locus)
      .setEnd(end.locus)
      .setReferenceAllele(allele.refBases.toString)
      .setAlternateAllele(allele.altBases.toString)
      .setContigName(contigName.name)
      .build

  def rsID: Option[Int]

  def toBDGGenotype: BDGGenotype
}
