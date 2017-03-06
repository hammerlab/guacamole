package org.hammerlab.guacamole.data

import org.hammerlab.guacamole.readsets.args.HasReference
import org.hammerlab.guacamole.util.TestUtil.resourcePath

object CancerWGSTestUtil extends HasReference {

  val bams: Array[String] =
    Array("normal.bam", "primary.bam", "recurrence.bam")
      .map(name ⇒ resourcePath(s"cancer-wgs1/$name"))

  val expectedSomaticCallsCSV = resourcePath("cancer-wgs1/variants.csv")

  override def referencePath: String = resourcePath("hg19.partial.fasta")
  override def referenceIsPartial: Boolean = true
}
