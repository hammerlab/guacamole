package org.bdgenomics.guacamole.callers

import org.bdgenomics.guacamole.{ MappedRead, TestUtil }
import org.bdgenomics.guacamole.TestUtil.SparkFunSuite
import org.bdgenomics.guacamole.pileup.Pileup
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.bdgenomics.guacamole.filters.GenotypeFilter

class SomaticLogOddsVariantCallerSuite extends SparkFunSuite with ShouldMatchers with TableDrivenPropertyChecks {

  def loadPileup(filename: String, locus: Long = 0): Pileup = {
    val records = TestUtil.loadReads(sc, filename).mappedReads
    val localReads = records.collect
    Pileup(localReads, locus)
  }

  /**
   * Common algorithm parameters - fixed for all tests
   */
  val logOddsThreshold = 90
  val minLikelihood = 30
  val minAlignmentQuality = 1
  val minReadDepth = 8
  val minAlternateReadDepth = 3
  val maxNormalAlternateReadDepth = 5
  val maxMappingComplexity = 20
  val minAlignmentForComplexity = 1

  val filterMultiAllelic = false

  val snvCorrelationPercent = 20
  val snvWindowRange = 25
  val minNormalReadDepth = 3

  def testVariants(tumorReads: Seq[MappedRead], normalReads: Seq[MappedRead], positions: Array[Long], isTrue: Boolean = false) = {
    val positionsTable = Table("locus", positions: _*)
    forAll(positionsTable) {
      (locus: Long) =>
        val (tumorPileup, normalPileup) = TestUtil.loadTumorNormalPileup(tumorReads, normalReads, locus)
        val calledGenotypes = SomaticLogOddsVariantCaller.callSomaticVariantsAtLocus(
          tumorPileup,
          normalPileup,
          logOddsThreshold,
          snvWindowRange,
          snvCorrelationPercent,
          minNormalReadDepth,
          maxNormalAlternateReadDepth,
          minAlternateReadDepth,
          maxMappingComplexity,
          minAlignmentForComplexity,
          minAlignmentQuality,
          filterMultiAllelic)
        val hasVariant = GenotypeFilter(calledGenotypes, minReadDepth, minAlternateReadDepth, minLikelihood).size > 0
        hasVariant should be(isTrue)
    }
  }

  sparkTest("testing simple positive variants") {
    val (tumorReads, normalReads) = TestUtil.loadTumorNormalReads(sc, "tumor.chr20.tough.sam", "normal.chr20.tough.sam")
    val positivePositions = Array[Long](39083205, 42999694, 25031215, 44061033, 45175149, 755754, 1843813,
      3555766, 3868620, 9896926, 14017900, 17054263, 35951019, 39083205, 50472935, 51858471, 58201903, 7087895,
      19772181, 30430960, 32150541, 42186626, 44973412, 46814443, 52311925, 53774355, 57280858, 62262870)
    testVariants(tumorReads, normalReads, positivePositions, isTrue = true)
  }

  sparkTest("testing simple negative variants on syn1") {
    val (tumorReads, normalReads) = TestUtil.loadTumorNormalReads(sc,
      "synthetic.challenge.set1.tumor.v2.withMDTags.chr2.syn1fp.sam",
      "synthetic.challenge.set1.normal.v2.withMDTags.chr2.syn1fp.sam")
    val negativePositions = Array[Long](216094721, 3529313, 8789794, 104043280, 104175801,
      126651101, 241901237, 57270796, 120757852)
    testVariants(tumorReads, normalReads, negativePositions, isTrue = false)
  }

  sparkTest("testing complex region negative variants on syn1") {
    val (tumorReads, normalReads) = TestUtil.loadTumorNormalReads(sc,
      "synthetic.challenge.set1.tumor.v2.withMDTags.chr2.complexvar.sam",
      "synthetic.challenge.set1.normal.v2.withMDTags.chr2.complexvar.sam")
    val negativePositions = Array[Long](148487667, 134307261, 90376213, 3638733, 112529049, 91662497, 109347468)
    testVariants(tumorReads, normalReads, negativePositions, isTrue = false)

    val positivePositions = Array[Long](82949713, 130919744)
    testVariants(tumorReads, normalReads, positivePositions, isTrue = true)
  }

  sparkTest("difficult negative variants") {

    val (tumorReads, normalReads) = TestUtil.loadTumorNormalReads(sc, "tumor.chr20.simplefp.sam", "normal.chr20.simplefp.sam")
    val negativeVariantPositions = Array[Long](26211835, 29603746, 29652479, 54495768, 13046318, 25939088)
    testVariants(tumorReads, normalReads, negativeVariantPositions, isTrue = false)
  }
}
