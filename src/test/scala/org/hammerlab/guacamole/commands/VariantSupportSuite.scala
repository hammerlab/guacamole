package org.hammerlab.guacamole.commands

import org.hammerlab.genomics.bases.{ Bases, BasesUtil }
import org.hammerlab.genomics.loci.parsing.ParsedLoci
import org.hammerlab.genomics.reference.test.LocusUtil
import org.hammerlab.genomics.reference.{ ContigName, Locus }
import org.hammerlab.guacamole.commands.VariantSupport.Caller.{ GenotypeCount, pileupsToAlleleCounts }
import org.hammerlab.guacamole.pileup.{ Pileup, Util ⇒ PileupUtil }
import org.hammerlab.guacamole.reads.MappedRead
import org.hammerlab.guacamole.readsets.io.TestInputConfig
import org.hammerlab.guacamole.readsets.rdd.ReadsRDDUtil
import org.hammerlab.guacamole.reference.ReferenceBroadcast
import org.hammerlab.guacamole.util.GuacFunSuite
import org.hammerlab.guacamole.util.TestUtil.resourcePath
import org.hammerlab.guacamole.windowing.SlidingWindow
import org.hammerlab.test.matchers.seqs.SetMatcher.setMatch
import org.scalatest.prop.TableDrivenPropertyChecks

class VariantSupportSuite
  extends GuacFunSuite
    with TableDrivenPropertyChecks
    with PileupUtil
    with ReadsRDDUtil
    with LocusUtil
    with BasesUtil {

  override lazy val reference =
    ReferenceBroadcast(
      resourcePath("grch37.partial.fasta"),
      sc,
      partialFasta = true
    )

  // TODO(ryan): move to BasesUtil
  implicit def convertStringStringT[T] = convertTuple3[String, String, T, Bases, Bases, T] _

  case class AllelesCount(ref: Bases, alt: Bases, count: Int)
  object AllelesCount {
    implicit def make(t: (String, String, Int)): AllelesCount = AllelesCount(t._1, t._2, t._3)
  }

  implicit def convertBasesLocusT[T] = convertTuple3[String, Int, T, ContigName, Locus, T] _
  case class Position(contigName: ContigName, locus: Locus, allelesCounts: Seq[AllelesCount])
  object Position {
    implicit def make[T](t: (String, Int, Seq[T]))(implicit fn: T => AllelesCount): Position =
      Position(t._1, t._2, t._3.map(fn))
  }

  def checkAlleleCounts(window: SlidingWindow[MappedRead],
                        positions: Position*): Unit =
    for {
      Position(contig, locus, alleleCounts) <- positions
    } {
      withClue(s"$contig:$locus") {

        window.setCurrentLocus(locus)

        val pileup = makePileup(window.currentRegions(), contig, locus)

        checkPileup(pileup, alleleCounts: _*)
      }
    }

  def checkPileup(pileup: Pileup, alleleCounts: AllelesCount*): Unit = {
    val computedAlleleCounts =
      for {
        GenotypeCount(_, _, _, ref, alternate, count) ← pileupsToAlleleCounts(Vector(pileup))
      } yield
        AllelesCount(ref, alternate, count)

    computedAlleleCounts.toSet should setMatch(alleleCounts)
  }

  def gatkReads(loci: String) =
    loadReadsRDD(
      sc,
      "gatk_mini_bundle_extract.bam",
      TestInputConfig(
        nonDuplicate = false,
        overlapsLoci = ParsedLoci(loci)
      )
    )
    .mappedReads
    .collect()
    .sortBy(_.start)

  def nonDuplicateGatkReads(loci: String) =
    loadReadsRDD(
      sc,
      "gatk_mini_bundle_extract.bam",
      TestInputConfig(
        nonDuplicate = true,
        overlapsLoci = ParsedLoci(loci)
      )
    )
    .mappedReads
    .collect()
    .sortBy(_.start)

  def pileup(contigName: ContigName, locus: Locus): Pileup =
    makePileup(gatkReads(s"$contigName:$locus-${locus.next}"), contigName, locus)

  test("read evidence for simple snvs") {
    checkPileup(
      pileup("20", 10008951),
      ("CACACACACACA", "C", 1), ("C", "C", 4)
    )
  }

  test("read evidence for mid-deletion") {
    checkPileup(
      pileup("20", 10006822),
      ("C", "", 6), ("C", "C", 2)
    )
  }

  test("read evidence for simple snvs 2") {
    checkPileup(
      pileup("20", 10000624),
      ("T", "T", 6), ("T", "C", 1)
    )
  }

  test("read evidence for simple snvs no filters") {
    val loci =
      Seq[Position](
        ("20",  9999900, Seq()),  // empty
        ("20",  9999995, Seq(("A", "ACT", 9))),
        ("20", 10007174, Seq(("C", "T", 5), ("C", "C", 3))),
        ("20", 10007175, Seq(("T", "T", 8)))
      )

    val window = SlidingWindow("20", 0, gatkReads("20:9999900-10007175").toIterator)
    checkAlleleCounts(window, loci: _*)
  }

  test("read evidence for simple snvs duplicate filtering") {
    val loci =
      Seq[Position](
        ("20",  9999995, Seq(("A", "ACT", 8))),
        ("20", 10006822, Seq(("C", "", 5), ("C", "C", 1)))
      )

    val window = SlidingWindow("20", 0, nonDuplicateGatkReads("20:9999995-10006822").toIterator)
    checkAlleleCounts(window, loci: _*)
  }
}
