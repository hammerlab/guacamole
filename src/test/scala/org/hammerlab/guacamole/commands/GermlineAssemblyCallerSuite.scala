package org.hammerlab.guacamole.commands

import org.hammerlab.guacamole.commands.GermlineAssemblyCaller.Arguments
import org.hammerlab.guacamole.util.{ TestUtil, GuacFunSuite }

class GermlineAssemblyCallerSuite extends GuacFunSuite {

  //  sparkTest("test assembly caller") {
  //    val input = "assemble-reads-set3-chr2-73613071.sam"
  //
  //
  //    val args = new Arguments
  //    args.reads = TestUtil.testDataPath(input)
  //    args.referenceFastaPath = TestUtil.testDataPath("chr2.fasta")
  //    args.parallelism = 2
  //    args.variantOutput = TestUtil.tmpFileName(suffix = ".vcf")
  //    args.snvWindowRange = 1000
  //
  //    GermlineAssemblyCaller.Caller.run(args, sc)
  //
  //  }

  sparkTest("test assembly caller: illumina platinum tests") {
    val input = "illumina-platinum-na12878/NA12878.10k_variants.plus_chr1_3M-3.1M.bam"

    val args = new Arguments
    args.reads = TestUtil.testDataPath(input)
    args.loci = "chr1:772754-772755"
    args.referenceFastaPath = TestUtil.testDataPath("illumina-platinum-na12878/chr1.prefix.fa")
    args.parallelism = 2
    args.variantOutput = TestUtil.tmpFileName(suffix = ".vcf")
    args.snvWindowRange = 50
    args.kmerSize = 35

    GermlineAssemblyCaller.Caller.run(args, sc)

  }

}
