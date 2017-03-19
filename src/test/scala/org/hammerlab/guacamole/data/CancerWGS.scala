package org.hammerlab.guacamole.data

import org.apache.hadoop.fs.Path
import org.hammerlab.genomics.readsets.args.base.HasReference
import org.hammerlab.guacamole.jointcaller.Inputs
import org.hammerlab.test.resources.File

object CancerWGS extends HasReference {

  val bams: Array[Path] =
    Array("normal.bam", "primary.bam", "recurrence.bam")
      .map(name ⇒ new Path(File(s"cancer-wgs1/$name")))

  val inputs =
    Inputs(
      bams,
      Vector("normal", "primary", "recurrence"),
      Vector("normal", "tumor", "tumor"),
      Vector("dna", "dna", "dna")
    )

  val expectedSomaticCallsCSV = File("cancer-wgs1/variants.csv")

  override def referencePath: String = File("hg19.partial.fasta")
  override def referenceIsPartial: Boolean = true
}
