package org.hammerlab.guacamole.data

import java.nio.file.Path

import org.hammerlab.genomics.readsets.args.base.HasReference
import org.hammerlab.guacamole.jointcaller.Inputs
import org.hammerlab.guacamole.jointcaller.Inputs.Arguments
import org.hammerlab.test.resources.{ File, PathUtil }

object CancerWGS
  extends HasReference
    with PathUtil {

  val bams: Array[Path] =
    Array("normal.bam", "primary.bam", "recurrence.bam")
      .map(name ⇒ File(s"cancer-wgs1/$name"): Path)

  val inputs =
    Inputs(
      new Arguments {
        override val paths = bams
        override val sampleNames = Array("normal", "primary", "recurrence")
        tissueTypes = Array("normal", "tumor", "tumor")
        analytes = Array("dna", "dna", "dna")
      }
    )

  val expectedSomaticCallsCSV = File("cancer-wgs1/variants.csv")

  override def referencePath: Path = "hg19.partial.fasta"
  override def referenceIsPartial: Boolean = true
}
