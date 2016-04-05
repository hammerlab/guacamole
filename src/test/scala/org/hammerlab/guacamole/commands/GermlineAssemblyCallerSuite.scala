package org.hammerlab.guacamole.commands

import org.apache.spark.SparkContext
import org.hammerlab.guacamole._
import org.hammerlab.guacamole.commands.GermlineAssemblyCaller.Arguments
import org.hammerlab.guacamole.reads.Read
import org.hammerlab.guacamole.reference.ReferenceGenome
import org.hammerlab.guacamole.util.TestUtil
import org.hammerlab.guacamole.variants.CalledAllele
import org.scalatest.{ BeforeAndAfterAll, FunSuite, Matchers }

class GermlineAssemblyCallerSuite extends FunSuite with Matchers with BeforeAndAfterAll {

  val args = new Arguments

  val input = NA12878TestUtils.na12878SubsetBam
  args.reads = TestUtil.testDataPath(input)
  args.parallelism = 2

  var sc: SparkContext = _
  var reference: ReferenceGenome = _
  var readSet: ReadSet = _

  override def beforeAll() {
    sc = Common.createSparkContext()
    reference = ReferenceGenome(referenceFastaPath)
  }

  override def afterAll(): Unit = {
    sc.stop()
  }

  val referenceFastaPath = TestUtil.testDataPath(NA12878TestUtils.chr1PrefixFasta)

  def verifyVariantsAtLocus(name: String,
                            locus: Int,
                            contig: String = "chr1",
                            kmerSize: Int = 31,
                            snvWindowRange: Int = 55,
                            minOccurrence: Int = 5,
                            minVaf: Float = 0.1f)(
                              expectedVariants: (String, Int, String, String)*): Unit = {

    val windowStart = locus - snvWindowRange
    val windowEnd = locus + snvWindowRange

    test(name) {
      val lociBuilder = LociSet.parse(s"$contig:$windowStart-$windowEnd")

      val readSet =
        Common.loadReadsFromArguments(
          args,
          sc,
          Read.InputFilters(
            mapped = true,
            nonDuplicate = true,
            overlapsLoci = Some(lociBuilder)
          )
        )

      val lociPartitions =
        DistributedUtil.partitionLociUniformly(
          tasks = args.parallelism,
          loci = lociBuilder.result(readSet.contigLengths)
        )

      val variants =
        GermlineAssemblyCaller.Caller.discoverGenotypes(
          readSet.mappedReads,
          kmerSize,
          snvWindowRange,
          minOccurrence,
          minVaf,
          reference,
          lociPartitions
        ).collect().sortBy(_.start)

      val actualVariants =
        for {
          CalledAllele(_, contig, start, allele, _, _, _) ← variants
        } yield {
          (contig, start, Bases.basesToString(allele.refBases), Bases.basesToString(allele.altBases))
        }

      actualVariants should be(expectedVariants)
    }

  }

  verifyVariantsAtLocus(
    "test assembly caller: illumina platinum tests; homozygous snp",
    772754
  )(
      ("chr1", 772754, "A", "C")
    )

  verifyVariantsAtLocus(
    "test assembly caller: illumina platinum tests; nearby homozygous snps",
    1297212
  )(
      ("chr1", 1297212, "G", "C"),
      ("chr1", 1297215, "A", "G")
    )

  verifyVariantsAtLocus(
    "test assembly caller: illumina platinum tests; 2 nearby homozygous snps",
    1316669
  )(
      ("chr1", 1316647, "C", "T"),
      ("chr1", 1316647, "C", "T"),
      ("chr1", 1316669, "C", "G"),
      ("chr1", 1316673, "C", "T")
    )

  verifyVariantsAtLocus(
    "test assembly caller: illumina platinum tests; het snp",
    1342611
  )(
      ("chr1", 1342611, "G", "C")
    )

  verifyVariantsAtLocus(
    "test assembly caller: illumina platinum tests; homozygous deletion",
    1296368
  )(
      ("chr1", 1296368, "GAC", "G")
    )

  verifyVariantsAtLocus(
    "test assembly caller: illumina platinum tests; homozygous deletion 2",
    1303426
  )(
      ("chr1", 1303426, "ACT", "A")
    )

  verifyVariantsAtLocus(
    "test assembly caller: illumina platinum tests; homozygous insertion",
    1321298
  )(
      ("chr1", 1321298, "A", "AG")
    )

  verifyVariantsAtLocus(
    "test assembly caller: illumina platinum tests; homozygous insertion 2",
    1302671
  )(
      ("chr1", 1302671, "A", "AGT")
    )

  verifyVariantsAtLocus(
    "test assembly caller: empty region",
    1303917
  )()

  verifyVariantsAtLocus(
    "test assembly caller: homozygous snp in a repeat region",
    789255
  )(
      ("chr1", 789255, "T", "C")
    )
}
