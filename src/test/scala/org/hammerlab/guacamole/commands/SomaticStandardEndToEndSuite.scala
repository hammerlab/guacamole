package org.hammerlab.guacamole.commands

import org.hammerlab.guacamole.util.TestUtil.resourcePath
import org.hammerlab.guacamole.variants.VCFCmpTest
import org.hammerlab.test.files.TmpFiles
import org.scalatest.FunSuite

class SomaticStandardEndToEndSuite
  extends FunSuite
    with TmpFiles
    with VCFCmpTest {

  test("simple variants, end to end") {

    val tmpOutputPath = tmpPath(suffix = ".vcf")

    val caller = SomaticStandard.Caller

    caller.setDefaultConf("spark.kryo.registrationRequired", "true")
    caller.setDefaultConf("spark.driver.host", "localhost")

    caller.run(
      "--normal-reads",
      resourcePath("normal.chr20.tough.sam"),
      "--tumor-reads",
      resourcePath("tumor.chr20.tough.sam"),
      "--reference",
      resourcePath("grch37.partial.fasta"),
      "--partial-reference",
      "--min-alignment-quality",
      "1",
      "--out",
      tmpOutputPath
    )

    checkVCFs(s"$tmpOutputPath/part-r-00000", resourcePath("tough.golden.vcf"))
  }
}
