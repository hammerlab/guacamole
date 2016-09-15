package org.hammerlab.guacamole.main

import org.apache.spark.SparkContext
import org.hammerlab.guacamole.commands.SomaticJoint.Arguments
import org.hammerlab.guacamole.commands.{SomaticJoint, SparkCommand}
import org.hammerlab.guacamole.data.{CancerWGSTestUtil, NA12878TestUtil}
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.util.TestUtil.resourcePath
import org.hammerlab.guacamole.variants.VariantComparisonTest

/**
 * Somatic joint-caller integration "tests" that output various statistics to stdout.
 *
 * To run:
 *
 *   mvn package -DskipTests
 *   mvn test-compile
 *   java \
 *     -Xmx4g \
 *     -cp "$(scripts/classpath)":target/scala-2.10.5/test-classes \
 *     org.hammerlab.guacamole.main.SomaticJointCallerIntegrationTests
 */
object SomaticJointCallerIntegrationTests
  extends SparkCommand[Arguments]
    with VariantComparisonTest {

  var tempFileNum = 0

  def tempFile(suffix: String): String = {
    tempFileNum += 1
    "/tmp/test-somatic-joint-caller-%d.vcf".format(tempFileNum)
  }

  override val name: String = "germline-assembly-integration-test"
  override val description: String = "output various statistics to stdout"

  val outDir = "/tmp/guacamole-somatic-joint-test"

  // The NA12878 tests use a 100MB BAM and ship all reads to executors, which makes for too-large tasks if we don't
  // increase the parallelism.
  setDefaultConf("spark.default.parallelism", "24")

  def main(args: Array[String]): Unit = {
    val args = new Arguments()
    args.outDir = outDir
    args.referencePath = CancerWGSTestUtil.referencePath
    args.referenceIsPartial = true
    args.somaticGenotypePolicy = "trigger"
    args.loci = ((1).until(22).map(i => "chr%d".format(i)) ++ Seq("chrX", "chrY")).mkString(",")

    args.paths = CancerWGSTestUtil.bams
    run(args)
  }

  override def run(args: Arguments, sc: SparkContext): Unit = {

    if (true) {
      println("somatic calling on subset of 3-sample cancer patient 1")

      if (true) {

        val forceCallLoci =
          LociSet(
            csvRecords(CancerWGSTestUtil.expectedSomaticCallsCSV)
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
        CancerWGSTestUtil.expectedSomaticCallsCSV,
        CancerWGSTestUtil.reference(sc),
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
        args.paths = Seq(NA12878TestUtil.subsetBam).toArray
        args.loci = "chr1:0-6700000"
        args.forceCallLociFile = NA12878TestUtil.expectedCallsVCF
        args.referencePath = NA12878TestUtil.chr1PrefixFasta
        SomaticJoint.Caller.run(args, sc)
      }

      println("************* GUACAMOLE *************")
      compareToVCF(resultFile, NA12878TestUtil.expectedCallsVCF)

      if (false) {
        println("************* UNIFIED GENOTYPER *************")
        compareToVCF(resourcePath(
          "illumina-platinum-na12878/unified_genotyper.vcf"),
          NA12878TestUtil.expectedCallsVCF)

        println("************* HaplotypeCaller *************")
        compareToVCF(resourcePath(
          "illumina-platinum-na12878/haplotype_caller.vcf"),
          NA12878TestUtil.expectedCallsVCF)
      }
    }
  }
}
