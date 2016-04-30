package org.hammerlab.guacamole.main

import org.apache.spark.SparkContext
import org.hammerlab.guacamole.NA12878TestUtils
import org.hammerlab.guacamole.commands.GermlineAssemblyCaller.Arguments
import org.hammerlab.guacamole.commands.{GermlineAssemblyCaller, SparkCommand}
import org.hammerlab.guacamole.util.TestUtil
import org.hammerlab.guacamole.variants.VariantComparisonTest

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
object GermlineAssemblyIntegrationTests extends SparkCommand[Arguments] with VariantComparisonTest {

  override val name: String = "germline-assembly-integration-test"
  override val description: String = "output various statistics to stdout"

  def main(args: Array[String]): Unit = run(args)

  override def run(args: Arguments, sc: SparkContext): Unit = {

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
