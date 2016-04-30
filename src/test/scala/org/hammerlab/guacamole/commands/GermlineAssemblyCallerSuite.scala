package org.hammerlab.guacamole.commands

import org.apache.spark.SparkContext
import org.hammerlab.guacamole._
import org.hammerlab.guacamole.commands.GermlineAssemblyCaller.Arguments
import org.hammerlab.guacamole.data.NA12878
import org.hammerlab.guacamole.distributed.LociPartitionUtils
import org.hammerlab.guacamole.loci.set.LociParser
import org.hammerlab.guacamole.reads.InputFilters
import org.hammerlab.guacamole.reference.ReferenceBroadcast
import org.hammerlab.guacamole.util.TestUtil
import org.hammerlab.guacamole.variants.CalledAllele
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

class GermlineAssemblyCallerSuite extends FunSuite with Matchers with BeforeAndAfterAll {

  val args = new Arguments

  val input = NA12878.subsetBam
  args.reads = TestUtil.testDataPath(input)
  args.parallelism = 1

  var sc: SparkContext = _
  var reference: ReferenceBroadcast = _
  var readSet: ReadSet = _

  override def beforeAll() {
    sc = Common.createSparkContext()
    reference = ReferenceBroadcast(referenceFastaPath, sc)
  }

  override def afterAll(): Unit = {
    sc.stop()
  }

  val referenceFastaPath = TestUtil.testDataPath(NA12878.chr1PrefixFasta)

  def verifyVariantsAtLocus(locus: Int,
                            contig: String = "chr1",
                            kmerSize: Int = 31,
                            snvWindowRange: Int = 45,
                            minOccurrence: Int = 5,
                            minVaf: Float = 0.1f,
                            shortcutAssembly: Boolean = false)(
                             expectedVariants: (String, Int, String, String)*
                           ) = {

    val windowStart = locus - snvWindowRange
    val windowEnd = locus + snvWindowRange

    val lociBuilder = LociParser(s"$contig:$windowStart-$windowEnd")

    val readSet =
      Common.loadReadsFromArguments(
        args,
        sc,
        InputFilters(
          mapped = true,
          nonDuplicate = true,
          overlapsLoci = lociBuilder
        )
      )

    val lociPartitions =
      LociPartitionUtils.partitionLociUniformly(
        tasks = args.parallelism,
        loci = lociBuilder.result(readSet.contigLengths)
      )

    val variants =
      GermlineAssemblyCaller.Caller.discoverGermlineVariants(
        readSet.mappedReads,
        kmerSize = kmerSize,
        snvWindowRange = snvWindowRange,
        minOccurrence = minOccurrence,
        minAreaVaf = minVaf,
        reference = reference,
        lociPartitions = lociPartitions,
        shortcutAssembly = shortcutAssembly
      ).collect().sortBy(_.start)

    val actualVariants =
      for {
        CalledAllele(_, contig, start, allele, _, _, _) <- variants
      } yield {
        (contig, start, Bases.basesToString(allele.refBases), Bases.basesToString(allele.altBases))
      }

    actualVariants should be(expectedVariants)

  }

  test (
    "test assembly caller: illumina platinum tests; homozygous snp") {
    verifyVariantsAtLocus(772754) (
      ("chr1", 772754, "A", "C")
    )
  }


  test (
    "test assembly caller: illumina platinum tests; homozygous snp; shortcut assembly") {
    verifyVariantsAtLocus(772754, shortcutAssembly = true) (
      ("chr1", 772754, "A", "C")
    )
  }

  test (
    "test assembly caller: illumina platinum tests; nearby homozygous snps") {
    verifyVariantsAtLocus(1297212) (
      ("chr1", 1297212, "G", "C"),
      ("chr1", 1297215, "A", "G")
    )
  }

  test (
    "test assembly caller: illumina platinum tests; 2 nearby homozygous snps") {
    verifyVariantsAtLocus(1316669) (
      ("chr1", 1316647, "C", "T"),
      ("chr1", 1316669, "C", "G"),
      ("chr1", 1316673, "C", "T")
    )
  }

  test (
    "test assembly caller: illumina platinum tests; 2 nearby homozygous snps; shortcut assembly") {
    verifyVariantsAtLocus(1316669, shortcutAssembly = true) (
      ("chr1", 1316647, "C", "T"),
      ("chr1", 1316669, "C", "G"),
      ("chr1", 1316673, "C", "T")
    )
  }

  test (
    "test assembly caller: illumina platinum tests; het snp") {
    verifyVariantsAtLocus(1342611) (
      ("chr1", 1342611, "G", "C")
    )
  }

  test (
    "test assembly caller: illumina platinum tests; homozygous deletion") {
    verifyVariantsAtLocus(1296368) (
      ("chr1", 1296368, "GAC", "G")
    )
  }

  test (
    "test assembly caller: illumina platinum tests; homozygous deletion; shortcut assembly") {
    verifyVariantsAtLocus(1296368, shortcutAssembly = true) (
      ("chr1", 1296368, "GAC", "G")
    )
  }

  test (
    "test assembly caller: illumina platinum tests; homozygous deletion 2") {
    verifyVariantsAtLocus(1303426) (
      ("chr1", 1303426, "ACT", "A")
    )
  }

  test (
    "test assembly caller: illumina platinum tests; homozygous insertion") {
    verifyVariantsAtLocus(1321298) (
      ("chr1", 1321298, "A", "AG")
    )
  }

  test (
    "test assembly caller: illumina platinum tests; homozygous insertion 2") {
    verifyVariantsAtLocus(1302671) (
      ("chr1", 1302671, "A", "AGT")
    )
  }

  test (
    "test assembly caller: empty region") {
    verifyVariantsAtLocus(1303917)()
  }

  test (
    "test assembly caller: homozygous snp in a repeat region") {
    verifyVariantsAtLocus(789255) (
      ("chr1", 789255, "T", "C")
    )
  }

  test (
      "test assembly caller: het variant near homozygous variant") {
      verifyVariantsAtLocus(743020, snvWindowRange = 55) (
        ("chr1", 743020, "T", "C"),
        ("chr1", 743071, "C", "A")
      )
  }

  test (
    "test assembly caller: het variant in between two homozygous variants") {
    verifyVariantsAtLocus(821925, snvWindowRange = 55) (
      ("chr1", 821886, "A", "G"),
      ("chr1", 821925, "C", "G"),
      ("chr1", 821947, "T", "C")
    )
  }
}
