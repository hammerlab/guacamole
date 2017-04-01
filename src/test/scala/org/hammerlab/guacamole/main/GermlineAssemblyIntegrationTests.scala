package org.hammerlab.guacamole.main

import org.apache.spark.SparkContext
import org.hammerlab.guacamole.commands.GermlineAssemblyCaller.Arguments
import org.hammerlab.guacamole.commands.GuacCommand
import org.hammerlab.guacamole.data.NA12878
import org.hammerlab.guacamole.variants.VariantComparisonTest
import org.hammerlab.test.resources.File

/**
 * Germline assembly caller integration "tests" that output various statistics to stdout.
 *
 * To run:
 *
 *   mvn package -DskipTests -Pguac,test
 *   scripts/guacamole-test GermlineAssemblyIntegrationTests
 */
object GermlineAssemblyIntegrationTests extends GuacCommand[Arguments] with VariantComparisonTest {

  override val name: String = "germline-assembly-integration-test"
  override val description: String = "output various statistics to stdout"

  import NA12878._

  override def main(args: Array[String]): Unit =
    run(
      "--reads", subsetBam.buildPath.toString,
      "--reference", chr1PrefixFasta.buildPath.toString,
      "--loci", "chr1:0-6700000",
      "--out", "/tmp/germline-assembly-na12878-guacamole-tests.vcf",
      "--partition-accuracy", "0",
      "--parallelism", "0",
      "--kmer-size", "31",
      "--assembly-window-range", "41",
      "--min-area-vaf", "40",
      "--shortcut-assembly",
      "--min-likelihood", "70",
      "--min-occurrence", "3"
    )

  override def run(args: Arguments, sc: SparkContext): Unit = {

    println("Germline assembly calling on subset of illumina platinum NA12878")

    val resultFile = args.outputPathOpt.get / "part-r-00000"

    println("************* GUACAMOLE GermlineAssembly *************")
    compareToVCF(resultFile, expectedCallsVCF)

    println("************* UNIFIED GENOTYPER *************")
    compareToVCF(
      File("illumina-platinum-na12878/unified_genotyper.vcf"),
      expectedCallsVCF
    )

    println("************* HaplotypeCaller *************")
    compareToVCF(
      File("illumina-platinum-na12878/haplotype_caller.vcf"),
      expectedCallsVCF
    )
  }
}
