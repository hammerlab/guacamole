package org.hammerlab.guacamole.commands

import org.hammerlab.genomics.reference.test.ClearContigNames
import org.hammerlab.guacamole.variants.VCFCmpTest
import org.hammerlab.test.Suite
import org.hammerlab.test.resources.File

class SomaticStandardEndToEndSuite
  extends Suite
    with ClearContigNames
    with VCFCmpTest {

  test("simple variants, end to end") {

    val tmpOutputPath = tmpPath(suffix = ".vcf")

    val caller = SomaticStandard.Caller

    caller.setDefaultConf("spark.kryo.registrationRequired", "true")
    caller.setDefaultConf("spark.driver.host", "localhost")
    caller.setDefaultConf("spark.ui.enabled", "false")

    caller.run(
      "--normal-reads",
      File("normal.chr20.tough.sam"),
      "--tumor-reads",
      File("tumor.chr20.tough.sam"),
      "--reference",
      "/Users/ryan/data/refs/hg19.fasta",
      //File("grch37.partial.fasta"),
      "--partial-reference",
      "--min-alignment-quality",
      "1",
      "--out", tmpOutputPath.toString
    )

    checkVCFs(tmpOutputPath / "part-r-00000", "tough.golden.vcf")
  }
}
