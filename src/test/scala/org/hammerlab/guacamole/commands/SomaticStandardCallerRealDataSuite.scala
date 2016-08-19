package org.hammerlab.guacamole.commands

import org.hammerlab.guacamole.filters.somatic.SomaticGenotypeFilter
import org.hammerlab.guacamole.pileup.{Util => PileupUtil}
import org.hammerlab.guacamole.reads.MappedRead
import org.hammerlab.guacamole.reference.{Locus, ReferenceBroadcast}
import org.hammerlab.guacamole.util.{GuacFunSuite, TestUtil}
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
      TestUtil.testDataPath("grch37.partial.fasta"),
      sc,
      partialFasta = true
    )

  /**
   * Common algorithm parameters - fixed for all tests
   */
  val logOddsThreshold = 120
  val minAlignmentQuality = 1
  val minTumorReadDepth = 8
  val minNormalReadDepth = 4
  val maxTumorReadDepth = 200
  val minTumorAlternateReadDepth = 3
  val maxMappingComplexity = 20
  val minAlignmentForComplexity = 1

  val filterMultiAllelic = false

  val minLikelihood = 70
  val minVAF = 5

  def testVariants(tumorReads: Seq[MappedRead],
                   normalReads: Seq[MappedRead],
                   positions: Array[Long],
                   shouldFindVariant: Boolean = false) = {
    positions.foreach((locus: Locus) => {
      val (tumorPileup, normalPileup) =
        loadTumorNormalPileup(
          tumorReads,
          normalReads,
          locus
        )

      val calledGenotypes =
        SomaticStandard.Caller.findPotentialVariantAtLocus(
          tumorPileup,
          normalPileup,
          logOddsThreshold,
          minAlignmentQuality,
          filterMultiAllelic
        )

      val foundVariant =
        SomaticGenotypeFilter(
          calledGenotypes,
          minTumorReadDepth,
          maxTumorReadDepth,
          minNormalReadDepth,
          minTumorAlternateReadDepth,
          logOddsThreshold,
          minVAF = minVAF,
          minLikelihood
        ).nonEmpty

      foundVariant should be(shouldFindVariant)
    })
  }

  test("testing simple positive variants") {
    val (tumorReads, normalReads) =
      loadTumorNormalReads(
        sc,
        "tumor.chr20.tough.sam",
        "normal.chr20.tough.sam"
      )

    val positivePositions = Array[Long](
        755754,
       1843813,
       3555766,
       3868620,
       7087895,
       9896926,
      14017900,
      17054263,
      19772181,
      25031215,
      30430960,
      32150541,
      35951019,
      42186626,
      42999694,
      44061033,
      44973412,
      45175149,
      46814443,
      50472935,
      51858471,
      52311925,
      53774355,
      57280858,
      58201903
    )

    testVariants(tumorReads, normalReads, positivePositions, shouldFindVariant = true)
  }

  test("testing simple negative variants on syn1") {
    val (tumorReads, normalReads) =
      loadTumorNormalReads(
        sc,
        "synthetic.challenge.set1.tumor.v2.withMDTags.chr2.syn1fp.sam",
        "synthetic.challenge.set1.normal.v2.withMDTags.chr2.syn1fp.sam"
      )

    val negativePositions = Array[Long](
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
        sc,
        "synthetic.challenge.set1.tumor.v2.withMDTags.chr2.complexvar.sam",
        "synthetic.challenge.set1.normal.v2.withMDTags.chr2.complexvar.sam"
      )

    val negativePositions = Array[Long](
      148487667,
      134307261,
       90376213,
        3638733,
      109347468
    )

    testVariants(tumorReads, normalReads, negativePositions, shouldFindVariant = false)

    val positivePositions = Array[Long](82949713, 130919744)
    testVariants(tumorReads, normalReads, positivePositions, shouldFindVariant = true)
  }

  test("difficult negative variants") {

    val (tumorReads, normalReads) =
      loadTumorNormalReads(
        sc,
        "tumor.chr20.simplefp.sam",
        "normal.chr20.simplefp.sam"
      )

    val negativeVariantPositions = Array[Long](
      13046318,
      25939088,
      26211835,
      29652479,
      54495768
    )

    testVariants(tumorReads, normalReads, negativeVariantPositions, shouldFindVariant = false)
  }
}
