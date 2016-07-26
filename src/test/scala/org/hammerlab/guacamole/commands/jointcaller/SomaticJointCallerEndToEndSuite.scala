package org.hammerlab.guacamole.commands.jointcaller

import java.io.File
import java.nio.file.Files

import org.apache.commons.io.FileUtils
import org.hammerlab.guacamole.util.TestUtil
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

class SomaticJointCallerEndToEndSuite extends FunSuite with Matchers with BeforeAndAfterAll {
  val cancerWGS1Bams = Vector("normal.tiny.bam", "primary.tiny.bam", "recurrence.tiny.bam").map(
    name => TestUtil.testDataPath("cancer-wgs1/" + name))

  def readFile(path: String): String =
    scala.io.Source.fromFile(path).getLines().filterNot(_.startsWith("##reference")).mkString("\n")

  val outDirPath = Files.createTempDirectory("TestUtil")
  val outDir = outDirPath.toString

  override def afterAll() {
    super.afterAll()
    FileUtils.deleteDirectory(new File(outDir))
  }

  test("end to end") {

    SomaticJoint.Caller.run(
      Array(
        "--loci-file", TestUtil.testDataPath("tiny.vcf"),
        "--force-call-loci-file", TestUtil.testDataPath("tiny.vcf"),
        "--reference-fasta", TestUtil.testDataPath("hg19.partial.fasta"),
        "--reference-fasta-is-partial",
        "--analytes", "dna", "dna", "dna",
        "--tissue-types", "normal", "tumor", "tumor",
        "--sample-names", "normal", "primary", "recurrence",
        "--out-dir", outDir
      ) ++ cancerWGS1Bams
    )

    readFile(outDir + "/all.vcf") should be(readFile(TestUtil.testDataPath("tiny-sjc-output/all.vcf")))
  }
}
