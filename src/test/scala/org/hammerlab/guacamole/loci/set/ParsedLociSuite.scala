package org.hammerlab.guacamole.loci.set

import org.hammerlab.guacamole.loci.parsing.ParsedLoci
import org.hammerlab.guacamole.util.GuacFunSuite

class ParsedLociSuite extends GuacFunSuite {
  // Loci-from-VCF sanity check.
  test("vcf loading") {
    ParsedLoci.fromArgs(
      lociStrOpt = None,
      lociFileOpt = Some("src/test/resources/truth.chr20.vcf"),
      sc.hadoopConfiguration
    ).get.result.count should be(743606)
  }
}
