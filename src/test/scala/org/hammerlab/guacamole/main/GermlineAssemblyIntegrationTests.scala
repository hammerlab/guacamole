package org.hammerlab.guacamole.main

import org.hammerlab.guacamole.VariantComparisonUtils.compareToVCF
import org.hammerlab.guacamole.commands.jointcaller.SomaticJoint
import org.hammerlab.guacamole.util.TestUtil
import org.hammerlab.guacamole.{Common, NA12878TestUtils}

object GermlineAssemblyIntegrationTests {


  def main(args: Array[String]): Unit = {

    val sc = Common.createSparkContext("GermlineAssemblyIntegrationTest")

    println("Germline assembly calling on subset of illumina platinum NA12878")

    val resultFile = tempFile(".vcf")

    val outDir = "/tmp/germline-assembly-na12878-guacamole-tests"


    val args = new GermlineAssembly.Caller.Arguments()
    args.out = resultFile
    args.paths = Seq(NA12878TestUtils.na12878SubsetBam).toArray
    args.loci = "chr1:0-6700000"
    args.forceCallLociFromFile = NA12878TestUtils.na12878ExpectedCallsVCF
    args.referenceFastaPath = NA12878TestUtils.chr1PrefixFasta
    SomaticJoint.Caller.run(args, sc)


    println("************* GUACAMOLE GermlineAssembly *************")
    compareToVCF(resultFile, NA12878TestUtils.na12878ExpectedCallsVCF)

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
