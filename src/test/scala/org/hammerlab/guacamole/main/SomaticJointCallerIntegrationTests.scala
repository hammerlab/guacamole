package org.hammerlab.guacamole.main

import org.hammerlab.guacamole.Common
import org.hammerlab.guacamole.VariantComparisonUtils.{compareToCSV, compareToVCF, csvRecords}
import org.hammerlab.guacamole.commands.jointcaller.SomaticJoint
import org.hammerlab.guacamole.data.{CancerWGS, NA12878}
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.util.TestUtil

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
object SomaticJointCallerIntegrationTests {

  var tempFileNum = 0

  def tempFile(suffix: String): String = {
    tempFileNum += 1
    "/tmp/test-somatic-joint-caller-%d.vcf".format(tempFileNum)
  }

  def main(args: Array[String]): Unit = {

    val sc = Common.createSparkContext("SomaticJointCallerIntegrationTest")

    println("somatic calling on subset of 3-sample cancer patient 1")
    val outDir = "/tmp/guacamole-somatic-joint-test"

    if (true) {
      if (true) {
        val args = new SomaticJoint.Arguments()
        args.outDir = outDir
        args.referenceFastaPath = CancerWGS.referenceFastaPath
        args.referenceFastaIsPartial = true
        args.somaticGenotypePolicy = "trigger"
        args.loci = ((1).until(22).map(i => "chr%d".format(i)) ++ Seq("chrX", "chrY")).mkString(",")

        args.paths = CancerWGS.bams.toArray

        val forceCallLoci =
          LociSet(
            csvRecords(CancerWGS.expectedSomaticCallsCSV)
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
        CancerWGS.expectedSomaticCallsCSV,
        CancerWGS.referenceBroadcast(sc),
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
        args.paths = Seq(NA12878.subsetBam).toArray
        args.loci = "chr1:0-6700000"
        args.forceCallLociFromFile = NA12878.expectedCallsVCF
        args.referenceFastaPath = NA12878.chr1PrefixFasta
        SomaticJoint.Caller.run(args, sc)
      }

      println("************* GUACAMOLE *************")
      compareToVCF(resultFile, NA12878.expectedCallsVCF)

      if (false) {
        println("************* UNIFIED GENOTYPER *************")
        compareToVCF(TestUtil.testDataPath(
          "illumina-platinum-na12878/unified_genotyper.vcf"),
          NA12878.expectedCallsVCF)

        println("************* HaplotypeCaller *************")
        compareToVCF(TestUtil.testDataPath(
          "illumina-platinum-na12878/haplotype_caller.vcf"),
          NA12878.expectedCallsVCF)
      }
    }
  }
}
