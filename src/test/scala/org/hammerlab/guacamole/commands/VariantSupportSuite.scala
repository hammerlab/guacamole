package org.hammerlab.guacamole.commands

import org.hammerlab.guacamole.LociSet
import org.hammerlab.guacamole.commands.VariantSupport.Caller.AlleleCount
import org.hammerlab.guacamole.pileup.Pileup
import org.hammerlab.guacamole.reads.{MappedRead, ReadInputFilters}
import org.hammerlab.guacamole.reference.ReferenceBroadcast
import org.hammerlab.guacamole.util.{GuacFunSuite, TestUtil}
import org.hammerlab.guacamole.windowing.SlidingWindow
import org.scalatest.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

class VariantSupportSuite extends GuacFunSuite with Matchers with TableDrivenPropertyChecks {

  def grch37Reference = ReferenceBroadcast(TestUtil.testDataPath("grch37.partial.fasta"), sc, partialFasta = true)

  def testAlleleCounts(window: SlidingWindow[MappedRead],
                       variantAlleleLoci: (String, Int, Seq[(String, String, Int)])*) = {
    for {
      (contig, locus, alleleCounts) <- variantAlleleLoci
    } {
      withClue(s"$contig:$locus") {

        window.setCurrentLocus(locus)

        val pileup =
          Pileup(
            window.currentRegions(),
            contig,
            locus,
            referenceContigSequence = grch37Reference.getContig(contig)
          )

        assertAlleleCounts(pileup, alleleCounts: _*)
      }
    }
  }

  def assertAlleleCounts(pileup: Pileup, alleleCounts: (String, String, Int)*): Unit = {
    val computedAlleleCounts =
      (for {
        AlleleCount(_, _, _, ref, alternate, count) <- VariantSupport.Caller.pileupToAlleleCounts(pileup)
      } yield (ref, alternate, count)
      )
        .toArray
        .sortBy(x => x)

    computedAlleleCounts should be(alleleCounts.sortBy(x => x))
  }

  def gatkReads(loci: String) =
    TestUtil.loadReads(
      sc,
      "gatk_mini_bundle_extract.bam",
      ReadInputFilters(
        mapped = true,
        nonDuplicate = false,
        overlapsLoci = LociSet.parse(loci)
      )
    ).mappedReads.collect().sortBy(_.start)

  def nonDuplicateGatkReads(loci: String) =
    TestUtil.loadReads(
      sc,
      "gatk_mini_bundle_extract.bam",
      ReadInputFilters(
        mapped = true,
        nonDuplicate = true,
        overlapsLoci = LociSet.parse(loci)
      )
    ).mappedReads.collect().sortBy(_.start)

  lazy val RnaReads =
    TestUtil
      .loadReads(sc, "rna_chr17_41244936.sam")
      .mappedReads
      .collect()
      .sortBy(_.start)

  sparkTest("read evidence for simple snvs") {
    val pileup = Pileup(gatkReads("20:10008951-10008952"), "20", 10008951, grch37Reference.getContig("20"))
    assertAlleleCounts(pileup, ("CACACACACACA", "C", 1), ("C", "C", 4))
  }

  sparkTest("read evidence for mid-deletion") {
    val pileup = Pileup(gatkReads("20:10006822-10006823"), "20", 10006822, referenceContigSequence = grch37Reference.getContig("20"))
    assertAlleleCounts(pileup, ("C", "", 6), ("C", "C", 2))
  }

  sparkTest("read evidence for simple snvs 2") {
    val reads = gatkReads("20:10000624-10000625")
    val pileup = Pileup(reads, "20", 10000624, referenceContigSequence = grch37Reference.getContig("20"))
    assertAlleleCounts(pileup, ("T", "T", 6), ("T", "C", 1))
  }

  sparkTest("read evidence for simple snvs no filters") {
    val loci =
      Seq(
        ("20", 9999900, Nil), // empty
        ("20", 9999995, Seq(("A", "ACT", 9))),
        ("20", 10007174, Seq(("C", "T", 5), ("C", "C", 3))),
        ("20", 10007175, Seq(("T", "T", 8)))
      )

    val window = SlidingWindow[MappedRead]("20", 0L, gatkReads("20:9999900-10007175").toIterator)
    testAlleleCounts(window, loci: _*)

  }

  sparkTest("read evidence for simple snvs duplicate filtering") {
    val loci =
      Seq(
        ("20", 9999995, Seq(("A", "ACT", 8))),
        ("20", 10006822, Seq(("C", "", 5), ("C", "C", 1)))
      )

    val window = SlidingWindow[MappedRead]("20", 0L, nonDuplicateGatkReads("20:9999995-10006822").toIterator)
    testAlleleCounts(window, loci: _*)
  }
}
