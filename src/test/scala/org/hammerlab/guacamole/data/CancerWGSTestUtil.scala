package org.hammerlab.guacamole.data

import org.apache.spark.SparkContext
import org.hammerlab.guacamole.reference.ReferenceBroadcast
import org.hammerlab.guacamole.util.TestUtil.resourcePath

object CancerWGSTestUtil {

  val bams =
    Seq("normal.bam", "primary.bam", "recurrence.bam")
      .map(name => resourcePath(s"cancer-wgs1/$name"))

  val expectedSomaticCallsCSV = resourcePath("cancer-wgs1/variants.csv")

  val referenceFastaPath = resourcePath("hg19.partial.fasta")
  def referenceBroadcast(sc: SparkContext) = ReferenceBroadcast(referenceFastaPath, sc, partialFasta = true)

}
