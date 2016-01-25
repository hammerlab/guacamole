package org.hammerlab.guacamole.commands

import org.bdgenomics.adam.rdd.ADAMContext
import org.hammerlab.guacamole.commands.GermlineAssemblyCaller.Arguments
import org.hammerlab.guacamole.util.{GuacFunSuite, TestUtil}
import org.scalatest.Matchers

class GermlineAssemblyCallerSuite extends GuacFunSuite with Matchers {

  sparkTest("test assembly caller: illumina platinum tests") {
    val input = "illumina-platinum-na12878/NA12878.10k_variants.plus_chr1_3M-3.1M.bam"

    val args = new Arguments
    args.reads = TestUtil.testDataPath(input)
    args.loci = "chr1:772754-772755"
    args.referenceFastaPath = TestUtil.testDataPath("illumina-platinum-na12878/chr1.prefix.fa")
    args.parallelism = 2
    args.variantOutput = TestUtil.tmpFileName(suffix = ".vcf")
    args.snvWindowRange = 50
    args.kmerSize = 45

    GermlineAssemblyCaller.Caller.run(args, sc)

    val variants = new ADAMContext(sc).loadVariants(args.variantOutput).collect()

    variants.length should be(1)
    val variant = variants(0)
    variant.getContig.getContigName should be ("chr1")
    variant.getStart should be(772754)
    variant.getReferenceAllele should be("A")
    variant.getAlternateAllele should be("C")

  }

}
