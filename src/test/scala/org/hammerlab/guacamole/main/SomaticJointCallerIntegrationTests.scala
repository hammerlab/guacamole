package org.hammerlab.guacamole.main

import org.hammerlab.guacamole.VariantComparisonUtils.{compareToCSV, compareToVCF, csvRecords}
import org.hammerlab.guacamole.commands.jointcaller.SomaticJoint
import org.hammerlab.guacamole.loci.set.{Builder => LociSetBuilder}
import org.hammerlab.guacamole.util.TestUtil
import org.hammerlab.guacamole.{CancerWGSTestUtils, Common, NA12878TestUtils}

// This app outputs a performance comparison. We may want to add assertions on the accuracy later.
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
        args.referenceFastaPath = CancerWGSTestUtils.referenceFastaPath
        args.referenceFastaIsPartial = true
        args.somaticGenotypePolicy = "trigger"
        args.loci = ((1).until(22).map(i => "chr%d".format(i)) ++ Seq("chrX", "chrY")).mkString(",")

        args.paths = CancerWGSTestUtils.cancerWGS1Bams.toArray
        val forceCallLoci = new LociSetBuilder
        csvRecords(CancerWGSTestUtils.cancerWGS1ExpectedSomaticCallsCSV).filter(!_.tumor.contains("decoy")).foreach(record => {
          forceCallLoci.put("chr" + record.contig,
            if (record.alt.nonEmpty) record.interbaseStart else record.interbaseStart - 1,
            if (record.alt.nonEmpty) record.interbaseStart + 1 else record.interbaseStart)
        })
        args.forceCallLoci = forceCallLoci.result.truncatedString(100000)

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
