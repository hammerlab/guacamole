package org.hammerlab.guacamole.commands

import org.apache.spark.SparkContext
import org.hammerlab.guacamole._
import org.hammerlab.guacamole.commands.GermlineAssemblyCaller.Arguments
import org.hammerlab.guacamole.reads.Read
import org.hammerlab.guacamole.reference.ReferenceBroadcast
import org.hammerlab.guacamole.util.TestUtil
import org.hammerlab.guacamole.variants.CalledAllele
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

class GermlineAssemblyCallerSuite extends FunSuite with Matchers with BeforeAndAfterAll {

  val args = new Arguments

  val input = NA12878TestUtils.na12878SubsetBam
  args.reads = TestUtil.testDataPath(input)
  args.parallelism = 2

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

  val referenceFastaPath = TestUtil.testDataPath(NA12878TestUtils.chr1PrefixFasta)

  def testFn(name: String,
             loci: String,
             kmerSize: Int = 31,
             snvWindowRange: Int = 55,
             minOccurrence: Int = 5,
             minVaf: Float = 0.1f)(
    expectedVariants: (String, Int, String, String)*
  ): Unit = {

    test(name) {
      val lociBuilder = LociSet.parse(loci)

      val readSet =
        Common.loadReadsFromArguments(
          args,
          sc,
          Read.InputFilters(
            mapped = true,
            nonDuplicate = true,
            overlapsLoci = Some(lociBuilder)
          ),
          reference = reference
        )

      val lociPartitions =
        DistributedUtil.partitionLociUniformly(
          tasks = args.parallelism,
          loci = lociBuilder.result(readSet.contigLengths)
        )

      val variants =
        GermlineAssemblyCaller.Caller.discoverGenotypes(
          readSet.mappedReads,
          kmerSize = kmerSize,
          snvWindowRange = snvWindowRange,
          minOccurrence = minOccurrence,
          minAreaVaf = minVaf,
          reference = reference,
          lociPartitions = lociPartitions
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

  testFn(
    "test assembly caller: illumina platinum tests; homozygous snp",
    "chr1:772754-772755"
  )(
    ("chr1", 772754, "A", "C")
  )

  testFn(
    "test assembly caller: illumina platinum tests; nearby homozygous snps",
    "chr1:1297212-1297213"
  )(
    ("chr1", 1297212, "G", "C"),
    ("chr1", 1297215, "A", "G")
  )

  testFn(
    "test assembly caller: illumina platinum tests; 2 nearby homozygous snps",
    "chr1:1316669-1316670"
  )(
    ("chr1", 1316647, "C", "T"),
    ("chr1", 1316669, "C", "G"),
    ("chr1", 1316673, "C", "T")
  )

  testFn(
    "test assembly caller: illumina platinum tests; het snp",
    "chr1:1342611-1342612"
  )(
    ("chr1", 1342611, "G", "C")
  )

  testFn(
    "test assembly caller: illumina platinum tests; homozygous deletion",
    "chr1:1296368-1296369"
  )(
    ("chr1", 1296368, "GAC", "G")
  )

  testFn(
    "test assembly caller: illumina platinum tests; homozygous deletion 2",
    "chr1:1303426-1303427"
  )(
    ("chr1", 1303426, "ACT", "A")
  )

  testFn(
    "test assembly caller: illumina platinum tests; homozygous insertion",
    "chr1:1321298-1321299"
  )(
    ("chr1", 1321298, "A", "AG")
  )

  testFn(
    "test assembly caller: illumina platinum tests; homozygous insertion 2",
    "chr1:1302671-1302672"
  )(
    ("chr1", 1302671, "A", "AGT")
  )

  testFn(
    "test assembly caller: empty region",
    "chr1:1303917-1303918"
  )()

  testFn(
    "test assembly caller: homozygous snp in a repeat region",
    "chr1:789255-789256"
  )(
    ("chr1", 789255, "T", "C")
  )
}
