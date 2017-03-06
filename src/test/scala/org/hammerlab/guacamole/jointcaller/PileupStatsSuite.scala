package org.hammerlab.guacamole.jointcaller

import org.hammerlab.genomics.bases.Base.{ A, C, G, N, T }
import org.hammerlab.guacamole.jointcaller.pileup_summarization.{ AlleleMixture, PileupStats }
import org.hammerlab.guacamole.pileup.{ Util ⇒ PileupUtil }
import org.hammerlab.guacamole.reads.ReadsUtil
import org.hammerlab.guacamole.reference.{ ReferenceBroadcast, ReferenceUtil }
import org.hammerlab.guacamole.util.GuacFunSuite
import org.hammerlab.test.resources.File

import scala.Seq.fill

class PileupStatsSuite
  extends GuacFunSuite
    with ReadsUtil
    with PileupUtil
    with ReferenceUtil {

  val cancerWGS1Bams = Seq("normal.bam", "primary.bam", "recurrence.bam").map(
    name ⇒ File("cancer-wgs1/" + name))

  val partialFasta = File("hg19.partial.fasta")
  def partialReference = {
    ReferenceBroadcast(partialFasta, sc, partialFasta = true)
  }

  val refString = "NTCGATCGA"
  override lazy val reference = makeReference("chr1", 0, refString)

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

    val pileups = (1 until refString.length).map(locus ⇒ makePileup(reads, "chr1", locus))

    val stats1 = PileupStats(pileups(1).elements, G)
    stats1.totalDepthIncludingReadsContributingNoAlleles should equal(6)
    stats1.allelicDepths should be(AllelicDepths(G → 6))
    stats1.nonRefAlleles should be(Nil)
    stats1.topAlt should ===(N)
    assert(stats1.logLikelihoodPileup(AlleleMixture(G → 1.0)) > stats1.logLikelihoodPileup(AlleleMixture(G → .99, C → .01)))
    assert(stats1.logLikelihoodPileup(AlleleMixture(T → 1.0)) < stats1.logLikelihoodPileup(AlleleMixture(G → .99, C → .01)))

    val stats2 = PileupStats(pileups(2).elements, A)
    stats2.allelicDepths should be(AllelicDepths(A → 2, C → 3, "ACCC" → 1))
    assert(stats2.nonRefAlleles === Seq("C", "ACCC"))
    assert(stats2.logLikelihoodPileup(AlleleMixture(A → 0.5, C → 0.5)) > stats2.logLikelihoodPileup(AlleleMixture(A → 1.0)))

    // True because of the higher base qualities on the C allele:
    assert(stats2.logLikelihoodPileup(AlleleMixture(C → 1.0)) > stats2.logLikelihoodPileup(AlleleMixture(A → 1.0)))

    val stats3 = PileupStats(pileups(3).elements, T)
    stats3.totalDepthIncludingReadsContributingNoAlleles should be(6)
    stats3.allelicDepths should be(AllelicDepths(T → 2))  // reads with an SNV at position 4 don't count
  }
}
