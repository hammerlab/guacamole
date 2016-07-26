package org.hammerlab.guacamole.loci

import org.hammerlab.guacamole.util.GuacFunSuite

class LociArgsSuite extends GuacFunSuite {
  // Loci-from-VCF sanity check.
  test("vcf loading") {
    val args = new LociArgs {}
    args.lociFile = "src/test/resources/truth.chr20.vcf"
    args.parseLoci(sc.hadoopConfiguration).result().count should be(743606)
  }
}
