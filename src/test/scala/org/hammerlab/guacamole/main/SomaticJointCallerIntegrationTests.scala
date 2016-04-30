package org.hammerlab.guacamole.main

import org.apache.spark.SparkContext
import org.hammerlab.guacamole.commands.SparkCommand
import org.hammerlab.guacamole.commands.jointcaller.SomaticJoint
import org.hammerlab.guacamole.commands.jointcaller.SomaticJoint.Arguments
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.util.TestUtil
import org.hammerlab.guacamole.variants.VariantComparisonTest
import org.hammerlab.guacamole.{CancerWGSTestUtils, NA12878TestUtils}

/**
 * Somatic joint-caller integration "tests" that output various statistics to stdout.
 *
 * To run:
 *
 *   mvn package
 *   mvn test-compile
 *   java \
 *     -cp target/guacamole-with-dependencies-0.0.1-SNAPSHOT.jar:target/scala-2.10.5/test-classes \
 *     org.hammerlab.guacamole.main.SomaticJointCallerIntegrationTests
 */
object SomaticJointCallerIntegrationTests extends SparkCommand[Arguments] with VariantComparisonTest {

  var tempFileNum = 0

  def tempFile(suffix: String): String = {
    tempFileNum += 1
    "/tmp/test-somatic-joint-caller-%d.vcf".format(tempFileNum)
  }

  override val name: String = "germline-assembly-integration-test"
  override val description: String = "output various statistics to stdout"

  def main(args: Array[String]): Unit = run(args)

  override def run(args: Arguments, sc: SparkContext): Unit = {

    println("somatic calling on subset of 3-sample cancer patient 1")
    val outDir = "/tmp/guacamole-somatic-joint-test"

    if (true) {
      if (true) {
        val args = new Arguments()
        args.outDir = outDir
        args.referenceFastaPath = CancerWGSTestUtils.referenceFastaPath
        args.referenceFastaIsPartial = true
        args.somaticGenotypePolicy = "trigger"
        args.loci = ((1).until(22).map(i => "chr%d".format(i)) ++ Seq("chrX", "chrY")).mkString(",")

        args.paths = CancerWGSTestUtils.cancerWGS1Bams.toArray

        val forceCallLoci =
          LociSet(
            csvRecords(CancerWGSTestUtils.cancerWGS1ExpectedSomaticCallsCSV)
              .filterNot(_.tumor.contains("decoy"))
              .map(record => {
                (
                  "chr" + record.contig,
                  if (record.alt.nonEmpty) record.interbaseStart else record.interbaseStart - 1L,
                  if (record.alt.nonEmpty) record.interbaseStart + 1L else record.interbaseStart
                )
              })
          )

        args.forceCallLoci = forceCallLoci.truncatedString(100000)

        SomaticJoint.Caller.run(args, sc)
      }
      println("************* CANCER WGS1 SOMATIC CALLS *************")

      compareToCSV(
        outDir + "/somatic.all_samples.vcf",
        CancerWGSTestUtils.cancerWGS1ExpectedSomaticCallsCSV,
        CancerWGSTestUtils.referenceBroadcast(sc),
        Set("primary", "recurrence")
      )
    }

    println("germline calling on subset of illumina platinum NA12878")
    if (true) {
      val resultFile = tempFile(".vcf")
      println(resultFile)

      if (true) {
        val args = new SomaticJoint.Arguments()
        args.out = resultFile
        args.paths = Seq(NA12878TestUtils.na12878SubsetBam).toArray
        args.loci = "chr1:0-6700000"
        args.forceCallLociFromFile = NA12878TestUtils.na12878ExpectedCallsVCF
        args.referenceFastaPath = NA12878TestUtils.chr1PrefixFasta
        SomaticJoint.Caller.run(args, sc)
      }

      println("************* GUACAMOLE *************")
      compareToVCF(resultFile, NA12878TestUtils.na12878ExpectedCallsVCF)

      if (false) {
        println("************* UNIFIED GENOTYPER *************")
        compareToVCF(TestUtil.testDataPath(
          "illumina-platinum-na12878/unified_genotyper.vcf"),
          NA12878TestUtils.na12878ExpectedCallsVCF)

        println("************* HaplotypeCaller *************")
        compareToVCF(TestUtil.testDataPath(
          "illumina-platinum-na12878/haplotype_caller.vcf"),
          NA12878TestUtils.na12878ExpectedCallsVCF)
      }
    }
  }
}
