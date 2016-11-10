package org.hammerlab.guacamole.commands

import org.hammerlab.guacamole.commands.VariantSupport.Caller.AlleleCount
import org.hammerlab.guacamole.loci.parsing.ParsedLoci
import org.hammerlab.guacamole.pileup.{Pileup, Util => PileupUtil}
import org.hammerlab.guacamole.reads.MappedRead
import org.hammerlab.guacamole.readsets.io.{InputConfig, TestInputConfig}
import org.hammerlab.guacamole.readsets.rdd.ReadsRDDUtil
import org.hammerlab.guacamole.reference.ReferenceBroadcast
import org.hammerlab.guacamole.util.GuacFunSuite
import org.hammerlab.guacamole.util.TestUtil.resourcePath
import org.hammerlab.guacamole.windowing.SlidingWindow
import org.scalatest.prop.TableDrivenPropertyChecks

class VariantSupportSuite
  extends GuacFunSuite
    with TableDrivenPropertyChecks
    with PileupUtil
    with ReadsRDDUtil {

  // Used implicitly by makePileup.
  override lazy val reference =
    ReferenceBroadcast(
      resourcePath("grch37.partial.fasta"),
      sc,
      partialFasta = true
    )

  def testAlleleCounts(window: SlidingWindow[MappedRead],
                       variantAlleleLoci: (String, Int, Seq[(String, String, Int)])*) = {
    for {
      (contig, locus, alleleCounts) <- variantAlleleLoci
    } {
      withClue(s"$contig:$locus") {

        window.setCurrentLocus(locus)

        val pileup = makePileup(window.currentRegions(), contig, locus)

        assertAlleleCounts(pileup, alleleCounts: _*)
      }
    }
  }

  def assertAlleleCounts(pileup: Pileup, alleleCounts: (String, String, Int)*): Unit = {
    val computedAlleleCounts =
      (for {
        AlleleCount(_, _, _, ref, alternate, count) <- VariantSupport.Caller.pileupsToAlleleCounts(Vector(pileup))
      } yield
        (ref, alternate, count)
      )
      .toArray
      .sortBy(x => x)

    computedAlleleCounts should be(alleleCounts.sortBy(x => x))
  }

  def gatkReads(loci: String) =
    loadReadsRDD(
      sc,
      "gatk_mini_bundle_extract.bam",
      TestInputConfig(
        nonDuplicate = false,
        overlapsLoci = ParsedLoci(loci)
      )
    ).mappedReads.collect().sortBy(_.start)

  def nonDuplicateGatkReads(loci: String) =
    loadReadsRDD(
      sc,
      "gatk_mini_bundle_extract.bam",
      TestInputConfig(
        nonDuplicate = true,
        overlapsLoci = ParsedLoci(loci)
      )
    ).mappedReads.collect().sortBy(_.start)

  lazy val RnaReads =
    loadReadsRDD(sc, "rna_chr17_41244936.sam")
      .mappedReads
      .collect()
      .sortBy(_.start)

  test("read evidence for simple snvs") {
    val pileup = makePileup(gatkReads("20:10008951-10008952"), "20", 10008951)
    assertAlleleCounts(pileup, ("CACACACACACA", "C", 1), ("C", "C", 4))
  }

  test("read evidence for mid-deletion") {
    val pileup = makePileup(gatkReads("20:10006822-10006823"), "20", 10006822)
    assertAlleleCounts(pileup, ("C", "", 6), ("C", "C", 2))
  }

  test("read evidence for simple snvs 2") {
    val reads = gatkReads("20:10000624-10000625")
    val pileup = makePileup(reads, "20", 10000624)
    assertAlleleCounts(pileup, ("T", "T", 6), ("T", "C", 1))
  }

  test("read evidence for simple snvs no filters") {
    val loci =
      Seq(
        ("20", 9999900, Nil),  // empty
        ("20", 9999995, Seq(("A", "ACT", 9))),
        ("20", 10007174, Seq(("C", "T", 5), ("C", "C", 3))),
        ("20", 10007175, Seq(("T", "T", 8)))
      )

    val window = SlidingWindow[MappedRead]("20", 0, gatkReads("20:9999900-10007175").toIterator)
    testAlleleCounts(window, loci: _*)
  }

  test("read evidence for simple snvs duplicate filtering") {
    val loci =
      Seq(
        ("20", 9999995, Seq(("A", "ACT", 8))),
        ("20", 10006822, Seq(("C", "", 5), ("C", "C", 1)))
      )

    val window = SlidingWindow[MappedRead]("20", 0, nonDuplicateGatkReads("20:9999995-10006822").toIterator)
    testAlleleCounts(window, loci: _*)
  }
}
