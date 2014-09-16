package org.bdgenomics.guacamole.callers

import org.bdgenomics.adam.cli.Args4j
import org.bdgenomics.guacamole.callers.SomaticThresholdVariantCaller.Arguments
import org.bdgenomics.guacamole.TestUtil.SparkFunSuite
import org.bdgenomics.guacamole.TestUtil

class SomaticThresholdVariantCallerSuite extends SparkFunSuite {
  val output = "/tmp/somatic.threshold.chr20.vcf"
  TestUtil.deleteIfExists(output)

  sparkTest("somatic threshold") {
    SomaticThresholdVariantCaller.runWrapper(
      sc,
      Args4j[Arguments](
        Array[String](
          "-tumor-reads", TestUtil.testDataPath("synth1.tumor.100k-200k.withmd.bam"),
          "-normal-reads", TestUtil.testDataPath("synth1.normal.100k-200k.withmd.bam"),
          "-parallelism", "20",
          "-loci", "20:100000-200000",
          "-out", output
        )
      )
    )
  }
}
