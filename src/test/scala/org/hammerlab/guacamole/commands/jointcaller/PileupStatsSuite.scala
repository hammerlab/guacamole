package org.hammerlab.guacamole.commands.jointcaller

import org.hammerlab.guacamole.commands.jointcaller.pileup_summarization.PileupStats
import org.hammerlab.guacamole.pileup.Pileup
import org.hammerlab.guacamole.reference.ReferenceBroadcast
import org.hammerlab.guacamole.util.{Bases, GuacFunSuite, TestUtil}

class PileupStatsSuite extends GuacFunSuite {
  val cancerWGS1Bams = Seq("normal.bam", "primary.bam", "recurrence.bam").map(
    name => TestUtil.testDataPath("cancer-wgs1/" + name))

  val partialFasta = TestUtil.testDataPath("hg19.partial.fasta")
  def partialReference = {
    ReferenceBroadcast(partialFasta, sc, partialFasta = true)
  }

  test("pileupstats likelihood computation") {
    val refString = "NTCGATCGA"
    def reference = TestUtil.makeReference(sc, Seq(("chr1", 0, refString)))
    def referenceContigSequence = reference.getContig("chr1")

    val reads = Seq(
      TestUtil.makeRead("TCGATCGA", "8M", 1, qualityScores = Some(Seq.fill(8)(10))),
      TestUtil.makeRead("TCGATCGA", "8M", 1, qualityScores = Some(Seq.fill(8)(20))),
      TestUtil.makeRead("TCGCTCGA", "8M", 1, qualityScores = Some(Seq.fill(8)(50))),
      TestUtil.makeRead("TCGCTCGA", "8M", 1, qualityScores = Some(Seq.fill(8)(50))),
      TestUtil.makeRead("TCGCTCGA", "8M", 1, qualityScores = Some(Seq.fill(8)(50))),
      TestUtil.makeRead("TCGACCCTCGA", "4M3I4M", 1, qualityScores = Some(Seq.fill(11)(30))))

    val pileups = (1 until refString.length).map(locus => Pileup(reads, "chr1", locus, referenceContigSequence))

    val stats1 = PileupStats.apply(pileups(1).elements, Bases.stringToBases("G"))
    stats1.totalDepthIncludingReadsContributingNoAlleles should equal(6)
    stats1.allelicDepths should equal(Map("G" -> 6))
    stats1.nonRefAlleles should equal(Seq.empty)
    stats1.topAlt should equal("N")
    assert(stats1.logLikelihoodPileup(Map("G" -> 1.0)) > stats1.logLikelihoodPileup(Map("G" -> .99, "C" -> .01)))
    assert(stats1.logLikelihoodPileup(Map("T" -> 1.0)) < stats1.logLikelihoodPileup(Map("G" -> .99, "C" -> .01)))

    val stats2 = PileupStats.apply(pileups(2).elements, Bases.stringToBases("A"))
    stats2.allelicDepths should equal(Map("A" -> 2, "C" -> 3, "ACCC" -> 1))
    stats2.nonRefAlleles should equal(Seq("C", "ACCC"))
    assert(stats2.logLikelihoodPileup(Map("A" -> 0.5, "C" -> 0.5)) > stats2.logLikelihoodPileup(Map("A" -> 1.0)))

    // True because of the higher base qualities on the C allele:
    assert(stats2.logLikelihoodPileup(Map("C" -> 1.0)) > stats2.logLikelihoodPileup(Map("A" -> 1.0)))

    val stats3 = PileupStats.apply(pileups(3).elements, Bases.stringToBases("T"))
    stats3.totalDepthIncludingReadsContributingNoAlleles should equal(6)
    stats3.allelicDepths should equal(Map("T" -> 2)) // reads with an SNV at position 4 don't count
  }
}
