package org.hammerlab.guacamole.data

import org.hammerlab.guacamole.readsets.args.HasReference
import org.hammerlab.test.resources.File

object CancerWGSTestUtil extends HasReference {

  val bams: Array[String] =
    Array("normal.bam", "primary.bam", "recurrence.bam")
      .map(name ⇒ File(s"cancer-wgs1/$name").path)

  val expectedSomaticCallsCSV = File("cancer-wgs1/variants.csv")

  override def referencePath: String = File("hg19.partial.fasta")
  override def referenceIsPartial: Boolean = true
}
