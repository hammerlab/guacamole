package org.hammerlab.guacamole.data

import org.apache.spark.SparkContext
import org.hammerlab.guacamole.reference.ReferenceBroadcast
import org.hammerlab.guacamole.util.TestUtil

object CancerWGS {

  val bams =
    Seq("normal.bam", "primary.bam", "recurrence.bam")
      .map(name => TestUtil.testDataPath(s"cancer-wgs1/$name"))

  val expectedSomaticCallsCSV = TestUtil.testDataPath("cancer-wgs1/variants.csv")

  val referenceFastaPath = TestUtil.testDataPath("hg19.partial.fasta")
  def referenceBroadcast(sc: SparkContext) = ReferenceBroadcast(referenceFastaPath, sc, partialFasta = true)

}
