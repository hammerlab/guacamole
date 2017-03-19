package org.hammerlab.guacamole.jointcaller

import org.hammerlab.genomics.readsets.io.Sample.{ Id, Name }
import org.hammerlab.genomics.readsets.io.{ Sample ⇒ RSSample }
import org.hammerlab.guacamole.jointcaller.Sample.{ Analyte, TissueType }

/**
 * An input BAM to the joint variant caller.
 *
 * The caller can work with any number of normal and tumor BAMs, each of which may be DNA or RNA.
 */
class Sample(val id: Id,
             val name: Name,
             val tissueType: TissueType.Value,
             val analyte: Analyte.Value)
  extends RSSample {

  // Some convenience properties.
  def normal = tissueType == TissueType.Normal
  def tumor = tissueType == TissueType.Tumor
  def dna = analyte == Analyte.DNA
  def rna = analyte == Analyte.RNA
  def normalDNA = normal && dna
  def tumorDNA = tumor && dna
  def normalRNA = normal && rna
  def tumorRNA = tumor && rna
}

object Sample {

  def unapply(sample: Sample): Option[(Id, Name, TissueType.Value, Analyte.Value)] =
    Some(
      sample.id,
      sample.name,
      sample.tissueType,
      sample.analyte
    )

  implicit def fromInput(input: Input): Sample = {
    val Input(id, name, _, tissueType, analyte) = input
    new Sample(id, name, tissueType, analyte)
  }

  /** Kind of tissue: tumor or normal. */
  object TissueType extends Enumeration {
    val Normal = Value("normal")
    val Tumor = Value("tumor")
  }

  /** Kind of sequencing: RNA or DNA. */
  object Analyte extends Enumeration {
    val DNA = Value("dna")
    val RNA = Value("rna")
  }
}
