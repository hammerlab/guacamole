package org.hammerlab.guacamole.main

import org.apache.spark.SparkContext
import org.hammerlab.genomics.loci.set.LociSet
import org.hammerlab.genomics.reference.{ Locus, Region }
import org.hammerlab.guacamole.commands.SomaticJoint.Arguments
import org.hammerlab.guacamole.commands.{ GuacCommand, SomaticJoint }
import org.hammerlab.guacamole.data.{ CancerWGSTestUtil, NA12878TestUtil }
import org.hammerlab.guacamole.reference.ReferenceBroadcast
import org.hammerlab.guacamole.variants.VariantComparisonTest
import org.hammerlab.test.resources.File

/**
 * Somatic joint-caller integration "tests" that output various statistics to stdout.
 *
 * To run:
 *
 *   mvn package -DskipTests -Pguac,test
 *   scripts/guacamole-test SomaticJointCallerIntegrationTests
 */
object SomaticJointCallerIntegrationTests
  extends GuacCommand[Arguments]
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

  override def main(args: Array[String]): Unit = {

    val forceCallLoci =
      LociSet(
        csvRecords(CancerWGSTestUtil.expectedSomaticCallsCSV)
          .filterNot(_.tumor.contains("decoy"))
          .map {
            record ⇒
              Region(
                "chr" + record.contig,
                Locus(if (record.alt.nonEmpty) record.interbaseStart else record.interbaseStart - 1L),
                Locus(if (record.alt.nonEmpty) record.interbaseStart + 1L else record.interbaseStart)
              )
          }
      )

    val args = new Arguments() {
      paths = CancerWGSTestUtil.bams
      referencePath = CancerWGSTestUtil.referencePath
      referenceIsPartial = true
      somaticGenotypePolicy = "trigger"
      lociStrOpt =
        Some(
          (
            (1 until 22).map(i ⇒ s"chr$i")
              ++ Seq("chrX", "chrY")
          )
          .mkString(",")
        )
      forceCallLociFileOpt = Some(forceCallLoci.toString(100000))
      outDir = outDir
    }

    run(args)
  }

  override def run(args: Arguments, sc: SparkContext): Unit = {

    if (true) {
      println("somatic calling on subset of 3-sample cancer patient 1")

      if (true) {
        SomaticJoint.Caller.run(args, sc)
      }
      println("************* CANCER WGS1 SOMATIC CALLS *************")

      compareToCSV(
        outDir + "/somatic.all_samples.vcf",
        CancerWGSTestUtil.expectedSomaticCallsCSV,
        ReferenceBroadcast(CancerWGSTestUtil, sc),
        Set("primary", "recurrence")
      )
    }

    println("germline calling on subset of illumina platinum NA12878")
    if (true) {
      val resultFile = tempFile(".vcf")
      println(resultFile)

      if (true) {
        val args = new SomaticJoint.Arguments() {
          out = resultFile
          paths = Seq(NA12878TestUtil.subsetBam.path).toArray
          lociStrOpt = Some("chr1:0-6700000")
          forceCallLociFileOpt = Some(NA12878TestUtil.expectedCallsVCF)
          referencePath = NA12878TestUtil.chr1PrefixFasta
        }

        SomaticJoint.Caller.run(args, sc)
      }

      println("************* GUACAMOLE *************")
      compareToVCF(resultFile, NA12878TestUtil.expectedCallsVCF)

      if (false) {
        println("************* UNIFIED GENOTYPER *************")
        compareToVCF(
          File("illumina-platinum-na12878/unified_genotyper.vcf"),
          NA12878TestUtil.expectedCallsVCF
        )

        println("************* HaplotypeCaller *************")
        compareToVCF(
          File("illumina-platinum-na12878/haplotype_caller.vcf"),
          NA12878TestUtil.expectedCallsVCF
        )
      }
    }
  }
}
