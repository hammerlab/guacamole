package org.hammerlab.guacamole.jointcaller

import org.hammerlab.genomics.reference.test.ClearContigNames
import org.hammerlab.guacamole.commands.SomaticJoint
import org.hammerlab.guacamole.variants.VCFCmpTest
import org.hammerlab.test.Suite
import org.hammerlab.test.resources.File

class SomaticJointCallerEndToEndSuite
  extends Suite
    with ClearContigNames
    with VCFCmpTest {

  val cancerWGS1Bams =
    Vector("normal.tiny.bam", "primary.tiny.bam", "recurrence.tiny.bam")
      .map(name ⇒ File("cancer-wgs1/" + name).pathStr)

  val outDir = tmpPath()

  test("end to end") {

    SomaticJoint.Caller.run(
      Array[String](
        "--loci-file", File("tiny.vcf"),
        "--force-call-loci-file", File("tiny.vcf"),
        "--reference", File("hg19.partial.fasta"),
        "--partial-reference",
        "--analytes", "dna,dna,dna",
        "--tissue-types", "normal,tumor,tumor",
        "--sample-names", "normal,primary,recurrence",
        "--include-duplicates",
        "--include-failed-quality-checks",
        "--out-dir", outDir.toString
      ) ++
        cancerWGS1Bams
    )

    checkVCFs(outDir / "all.vcf", "tiny-sjc-output/all.vcf")
  }
}
