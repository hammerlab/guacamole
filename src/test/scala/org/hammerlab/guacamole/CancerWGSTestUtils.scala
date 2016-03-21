package org.hammerlab.guacamole

import org.apache.spark.SparkContext
import org.hammerlab.guacamole.reference.ReferenceBroadcast
import org.hammerlab.guacamole.util.TestUtil

object CancerWGSTestUtils {

  val cancerWGS1Bams = Seq("normal.bam", "primary.bam", "recurrence.bam").map(
    name => TestUtil.testDataPath("cancer-wgs1/" + name))

  val cancerWGS1ExpectedSomaticCallsCSV = TestUtil.testDataPath("cancer-wgs1/variants.csv")

  val referenceFastaPath = TestUtil.testDataPath("hg19.partial.fasta")
  def referenceBroadcast(sc: SparkContext) = ReferenceBroadcast(referenceFastaPath, sc, partialFasta = true)

}
