package org.hammerlab.guacamole.jointcaller

import org.hammerlab.guacamole.jointcaller.pileup_summarization.PileupStats
import org.hammerlab.guacamole.pileup.{ Util ⇒ PileupUtil }
import org.hammerlab.guacamole.reads.ReadsUtil
import org.hammerlab.guacamole.reference.{ ReferenceBroadcast, ReferenceUtil }
import org.hammerlab.guacamole.util.Bases.stringToBases
import org.hammerlab.guacamole.util.GuacFunSuite
import org.hammerlab.guacamole.util.TestUtil.resourcePath

import scala.Seq.fill

class PileupStatsSuite
  extends GuacFunSuite
    with ReadsUtil
    with PileupUtil
    with ReferenceUtil {

  val cancerWGS1Bams = Seq("normal.bam", "primary.bam", "recurrence.bam").map(
    name => resourcePath("cancer-wgs1/" + name))

  val partialFasta = resourcePath("hg19.partial.fasta")
  def partialReference = {
    ReferenceBroadcast(partialFasta, sc, partialFasta = true)
  }

  val refString = "NTCGATCGA"
  override lazy val reference = makeReference(sc, "chr1", 0, refString)

  test("pileupstats likelihood computation") {

    val reads =
      Seq(
        makeRead(   "TCGATCGA",     "8M", qualityScores = fill( 8)(10)),
        makeRead(   "TCGATCGA",     "8M", qualityScores = fill( 8)(20)),
        makeRead(   "TCGCTCGA",     "8M", qualityScores = fill( 8)(50)),
        makeRead(   "TCGCTCGA",     "8M", qualityScores = fill( 8)(50)),
        makeRead(   "TCGCTCGA",     "8M", qualityScores = fill( 8)(50)),
        makeRead("TCGACCCTCGA", "4M3I4M", qualityScores = fill(11)(30))
      )

    val pileups = (1 until refString.length).map(locus => makePileup(reads, "chr1", locus))

    val stats1 = PileupStats(pileups(1).elements, stringToBases("G"))
    stats1.totalDepthIncludingReadsContributingNoAlleles should be(6)
    stats1.allelicDepths should be(Map("G" → 6))
    stats1.nonRefAlleles should be(Nil)
    stats1.topAlt should be("N")
    assert(stats1.logLikelihoodPileup(Map("G" → 1.0)) > stats1.logLikelihoodPileup(Map("G" → .99, "C" → .01)))
    assert(stats1.logLikelihoodPileup(Map("T" → 1.0)) < stats1.logLikelihoodPileup(Map("G" → .99, "C" → .01)))

    val stats2 = PileupStats(pileups(2).elements, stringToBases("A"))
    stats2.allelicDepths should be(Map("A" → 2, "C" → 3, "ACCC" → 1))
    stats2.nonRefAlleles should be(Seq("C", "ACCC"))
    assert(stats2.logLikelihoodPileup(Map("A" → 0.5, "C" → 0.5)) > stats2.logLikelihoodPileup(Map("A" → 1.0)))

    // True because of the higher base qualities on the C allele:
    assert(stats2.logLikelihoodPileup(Map("C" -> 1.0)) > stats2.logLikelihoodPileup(Map("A" -> 1.0)))

    val stats3 = PileupStats(pileups(3).elements, stringToBases("T"))
    stats3.totalDepthIncludingReadsContributingNoAlleles should be(6)
    stats3.allelicDepths should be(Map("T" → 2))  // reads with an SNV at position 4 don't count
  }
}
