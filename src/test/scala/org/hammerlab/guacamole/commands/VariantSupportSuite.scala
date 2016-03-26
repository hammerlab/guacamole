package org.hammerlab.guacamole.commands

import org.hammerlab.guacamole.Bases
import org.hammerlab.guacamole.pileup.Pileup
import org.hammerlab.guacamole.reads.{ MappedRead, Read }
import org.hammerlab.guacamole.reference.ReferenceBroadcast
import org.hammerlab.guacamole.util.{ GuacFunSuite, TestUtil }
import org.hammerlab.guacamole.windowing.SlidingWindow
import org.scalatest.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

class VariantSupportSuite extends GuacFunSuite with Matchers with TableDrivenPropertyChecks {

  def grch37Reference = ReferenceBroadcast(TestUtil.testDataPath("grch37.partial.fasta"), sc, partialFasta = true)

  def testAlleleCounts(window: SlidingWindow[MappedRead],
                       variantAlleleLoci: (String, Long, Map[String, Int])*) = {
    val variantAlleleLociTable = Table(
      heading = ("contig", "locus", "alleleCounts"),
      variantAlleleLoci: _*
    )

    forAll(variantAlleleLociTable) {
      (contig: String, locus: Long, alleleCountMap) =>
        {
          window.setCurrentLocus(locus)
          val pileup = Pileup(window.currentRegions(), contig, locus, referenceContigSequence = grch37Reference.getContig(contig))
          assertAlleleCounts(pileup, alleleCountMap)
        }
    }
  }

  def assertAlleleCounts(pileup: Pileup,
                         alleleCountMap: Map[String, Int]) = {
    val computedAlleleCounts = VariantSupport.Caller.pileupToAlleleCounts(pileup)
    computedAlleleCounts.size should be(alleleCountMap.size)

    for (alleleCount <- computedAlleleCounts) {
      alleleCount.count should be(alleleCountMap(alleleCount.alternate))
    }
  }

  lazy val gatkReads = TestUtil.loadReads(
    sc,
    "gatk_mini_bundle_extract.bam",
    Read.InputFilters(mapped = true, nonDuplicate = false),
    reference = grch37Reference).mappedReads.collect().sortBy(_.start)

  lazy val nonDuplicateGatkReads = TestUtil.loadReads(
    sc,
    "gatk_mini_bundle_extract.bam",
    Read.InputFilters(mapped = true, nonDuplicate = true),
    reference = grch37Reference
  ).mappedReads.collect().sortBy(_.start)

  lazy val RnaReads = TestUtil.loadReads(
    sc,
    "rna_chr17_41244936.sam",
    reference = grch37Reference).mappedReads.collect().sortBy(_.start)

  sparkTest("read evidence for simple snvs") {
    val pileup = Pileup(gatkReads, "20", 10008951, grch37Reference.getContig("20"))
    assertAlleleCounts(pileup, Map(("A", 1), ("C", 4)))
  }

  sparkTest("read evidence for mid-deletion") {

    val pileup = Pileup(gatkReads, "20", 10006822, referenceContigSequence = grch37Reference.getContig("20"))
    assertAlleleCounts(pileup, Map(("", 5), ("C", 1)))
  }

  sparkTest("read evidence for simple snvs 2") {
    val pileup = Pileup(gatkReads, "20", 10009053, referenceContigSequence = grch37Reference.getContig("20"))
    assertAlleleCounts(pileup, Map(("AT", 3)))
  }

  sparkTest("read evidence for simple snvs 3") {
    val loci = Seq(
      ("20", 10006822L, Map(("A", 2), ("", 6))),
      ("20", 10008951L, Map(("A", 1), ("C", 4))),
      ("20", 10009053L, Map(("AT", 3)))
    )

    val window = SlidingWindow[MappedRead]("20", 0L, gatkReads.toIterator)
    testAlleleCounts(window, loci: _*)
  }

  sparkTest("read evidence for simple snvs no filters") {
    val loci = Seq(
      ("20", 1L, Map[String, Int]()), // empty
      ("20", 9999996L, Map(("ACT", 9))),
      ("20", 10007174L, Map(("T", 5), ("C", 3))),
      ("20", 10260442L, Map(("T", 7)))
    )

    val window = SlidingWindow[MappedRead]("20", 0L, gatkReads.toIterator)
    testAlleleCounts(window, loci: _*)

  }

  sparkTest("read evidence for simple snvs duplicate filtering") {
    val loci = Seq(
      ("20", 9999996L, Map(("ACT", 8))),
      ("20", 10006822L, Map(("", 5), ("C", 1))),
      ("20", 10008920L, Map(("C", 2), ("CA", 1), ("CAA", 1))),
      ("20", 10009053L, Map(("AT", 3)))
    )

    val window = SlidingWindow[MappedRead]("20", 0L, nonDuplicateGatkReads.toIterator)
    testAlleleCounts(window, loci: _*)
  }
}