package org.hammerlab.guacamole.jointcaller

import org.hammerlab.guacamole.commands.SomaticJoint
import org.hammerlab.guacamole.util.TestUtil.resourcePath
import org.hammerlab.guacamole.variants.VCFCmpTest
import org.hammerlab.magic.test.TmpFiles
import org.scalatest.{FunSuite, Matchers}

class SomaticJointCallerEndToEndSuite
  extends FunSuite
    with Matchers
    with TmpFiles
    with VCFCmpTest {

  val cancerWGS1Bams = Vector("normal.tiny.bam", "primary.tiny.bam", "recurrence.tiny.bam").map(
    name => resourcePath("cancer-wgs1/" + name))

  val outDir = tmpPath()

  test("end to end") {

    SomaticJoint.Caller.run(
      Array(
        "--loci-file", resourcePath("tiny.vcf"),
        "--force-call-loci-file", resourcePath("tiny.vcf"),
        "--reference-fasta", resourcePath("hg19.partial.fasta"),
        "--reference-fasta-is-partial",
        "--analytes", "dna", "dna", "dna",
        "--tissue-types", "normal", "tumor", "tumor",
        "--sample-names", "normal", "primary", "recurrence",
        "--out-dir", outDir
      ) ++ cancerWGS1Bams
    )

    checkVCFs(s"$outDir/all.vcf", resourcePath("tiny-sjc-output/all.vcf"))
  }
}
