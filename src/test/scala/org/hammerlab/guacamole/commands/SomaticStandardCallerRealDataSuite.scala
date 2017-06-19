package org.hammerlab.guacamole.commands

import org.hammerlab.genomics.reads.MappedRead
import org.hammerlab.genomics.reference.Locus
import org.hammerlab.guacamole.commands.SomaticStandard.Caller.findPotentialVariantAtLocus
import org.hammerlab.guacamole.filters.somatic.TestSomaticGenotypeFilter
import org.hammerlab.guacamole.pileup.{ Util ⇒ PileupUtil }
import org.hammerlab.guacamole.reference.ReferenceBroadcast
import org.hammerlab.guacamole.util.GuacFunSuite
import org.hammerlab.test.resources.File
import org.scalatest.prop.TableDrivenPropertyChecks

/**
 * SomaticStandardCaller test cases that load real data from sample BAM files.
 */
class SomaticStandardCallerRealDataSuite
  extends GuacFunSuite
    with TableDrivenPropertyChecks
    with PileupUtil {

  lazy val reference =
    ReferenceBroadcast(
      File("grch37.partial.fasta"),
      sc,
      partialFasta = true
    )

  /**
   * Common algorithm parameters - fixed for all tests
   */
  val logOddsThreshold = 7
  val minTumorReadDepth = 8
  val minNormalReadDepth = 4
  val maxTumorReadDepth = 200
  val minTumorAlternateReadDepth = 3

  val filterMultiAllelic = false

  val minLikelihood = 0
  val minVAF = 5

  def testVariants(tumorReads: Seq[MappedRead],
                   normalReads: Seq[MappedRead],
                   positions: Array[Locus],
                   shouldFindVariant: Boolean = false) = {
    val positionsTable = Table("locus", positions: _*)
    forAll(positionsTable) {
      locus ⇒
        val (tumorPileup, normalPileup) =
          loadTumorNormalPileup(
            tumorReads,
            normalReads,
            locus
          )

        val calledGenotypes =
          findPotentialVariantAtLocus(
            tumorPileup,
            normalPileup,
            normalOddsThreshold = logOddsThreshold,
            tumorOddsThreshold = logOddsThreshold
          ).toSeq

        val foundVariant =
          TestSomaticGenotypeFilter(
            calledGenotypes,
            minTumorReadDepth,
            maxTumorReadDepth,
            minNormalReadDepth,
            minTumorAlternateReadDepth,
            logOddsThreshold,
            minVAF = minVAF,
            minLikelihood = minLikelihood
          ).nonEmpty

        foundVariant should be(shouldFindVariant)
    }
  }

  test("testing simple negative variants on syn1") {
    val (tumorReads, normalReads) =
      loadTumorNormalReads(
        "synthetic.challenge.set1.tumor.v2.withMDTags.chr2.syn1fp.sam",
        "synthetic.challenge.set1.normal.v2.withMDTags.chr2.syn1fp.sam"
      )

    val negativePositions =
      Array(
        216094721,
          3529313,
          8789794,
        104043280,
        104175801,
        126651101,
        241901237,
         57270796,
        120757852
      )

    testVariants(tumorReads, normalReads, negativePositions, shouldFindVariant = false)
  }

  test("testing complex region negative variants on syn1") {
    val (tumorReads, normalReads) =
      loadTumorNormalReads(
        "synthetic.challenge.set1.tumor.v2.withMDTags.chr2.complexvar.sam",
        "synthetic.challenge.set1.normal.v2.withMDTags.chr2.complexvar.sam"
      )

    val negativePositions =
      Array(
        // 148487667, This is a negative variant, though we can't determine that from the pileup
        //            Instead, we could examine the reads containing the variant bases and see they are misaligned
        134307261,
         90376213,
          3638733,
        109347468
      )

    testVariants(tumorReads, normalReads, negativePositions, shouldFindVariant = false)

    val positivePositions =
      Array(
         82949713,
        130919744
      )

    testVariants(tumorReads, normalReads, positivePositions, shouldFindVariant = true)
  }

  test("difficult negative variants") {

    val (tumorReads, normalReads) =
      loadTumorNormalReads(
        "tumor.chr20.simplefp.sam",
        "normal.chr20.simplefp.sam"
      )

    val negativeVariantPositions =
      Array(
        13046318,
        25939088,
        26211835,
        29652479,
        54495768
      )

    testVariants(tumorReads, normalReads, negativeVariantPositions, shouldFindVariant = false)
  }
}
