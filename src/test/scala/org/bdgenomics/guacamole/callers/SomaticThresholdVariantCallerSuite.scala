package org.bdgenomics.guacamole.callers

import org.bdgenomics.guacamole.TestUtil.SparkFunSuite
import org.bdgenomics.guacamole.TestUtil

class SomaticThresholdVariantCallerSuite extends SparkFunSuite {
  val output = "/tmp/somatic.threshold.chr20.vcf"
  TestUtil.deleteIfExists(output)
  val caller = SomaticThresholdVariantCaller
  caller.run(
    Array[String](
      "-tumor-reads", TestUtil.testDataPath("synth1.tumor.100k-200k.withmd.bam"),
      "-normal-reads", TestUtil.testDataPath("synth1.normal.100k-200k.withmd.bam"),
      "-loci", "20:100000-200000",
      "-parallelism", "20",
      "-out", output))
}
