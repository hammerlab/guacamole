package org.hammerlab.guacamole.main

import org.hammerlab.guacamole.VariantComparisonUtils.compareToVCF
import org.hammerlab.guacamole.commands.GermlineAssemblyCaller
import org.hammerlab.guacamole.util.TestUtil
import org.hammerlab.guacamole.{Common, NA12878TestUtils}

/**
 * Germline assembly caller integration "tests" that output various statistics to stdout.
 *
 * To run:
 *
 *   mvn package
 *   mvn test-compile
 *   java \
 *     -cp target/guacamole-with-dependencies-0.0.1-SNAPSHOT.jar:target/scala-2.10.5/test-classes \
 *     org.hammerlab.guacamole.main.GermlineAssemblyIntegrationTests
 */
object GermlineAssemblyIntegrationTests {

  def main(args: Array[String]): Unit = {

    val sc = Common.createSparkContext("GermlineAssemblyIntegrationTest")

    println("Germline assembly calling on subset of illumina platinum NA12878")
    val args = new GermlineAssemblyCaller.Arguments()
    // Input/output config
    args.reads = NA12878TestUtils.na12878SubsetBam
    args.referenceFastaPath = NA12878TestUtils.chr1PrefixFasta
    args.loci = "chr1:0-6700000"
    args.variantOutput = "/tmp/germline-assembly-na12878-guacamole-tests.vcf"


    // Read loading config
    args.bamReaderAPI = "hadoopbam"
    args.partitioningAccuracy = 0
    args.parallelism = 0

    // Germline assembly config
    args.kmerSize = 31
    args.snvWindowRange = 41
    args.minAreaVaf = 40
    args.shortcutAssembly = true
    args.minLikelihood = 70
    args.minOccurrence = 3

    GermlineAssemblyCaller.Caller.run(args, sc)

    val resultFile = args.variantOutput + "/part-r-00000"
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
