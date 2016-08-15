package org.hammerlab.guacamole.commands

import org.hammerlab.guacamole.filters.SomaticGenotypeFilter
import org.hammerlab.guacamole.pileup.Pileup
import org.hammerlab.guacamole.reads.{MappedRead, ReadsUtil}
import org.hammerlab.guacamole.reference.{ContigName, Locus, ReferenceBroadcast}
import org.hammerlab.guacamole.util.{Bases, GuacFunSuite, TestUtil}
import org.scalatest.prop.TableDrivenPropertyChecks

class SomaticStandardCallerSuite
  extends GuacFunSuite
    with TableDrivenPropertyChecks
    with ReadsUtil {

  def grch37Reference = ReferenceBroadcast(TestUtil.testDataPath("grch37.partial.fasta"), sc, partialFasta = true)

  def simpleReference = TestUtil.makeReference(sc, Seq(
    ("chr1", 0, "TCGATCGACG"),
    ("chr2", 0, "TCGAAGCTTCG"),
    ("chr3", 10, "TCGAATCGATCGATCGA"),
    ("chr4", 0, "TCGAAGCTTCGAAGCT")))

  def loadPileup(filename: String, contig: String, locus: Locus = 0): Pileup = {
    val contigReference = grch37Reference.getContig(contig)
    val records = TestUtil.loadReads(sc, filename).mappedReads
    val localReads = records.collect
    Pileup(localReads, contig, locus, contigReference)
  }

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

  def testVariants(tumorReads: Seq[MappedRead], normalReads: Seq[MappedRead], positions: Array[Long], shouldFindVariant: Boolean = false) = {
    positions.foreach((locus: Locus) => {
        val (tumorPileup, normalPileup) =
          TestUtil.loadTumorNormalPileup(
            tumorReads,
            normalReads,
            locus,
            reference = grch37Reference
          )

        val calledGenotypes =
          SomaticStandard.Caller.findPotentialVariantAtLocus(
            tumorPileup,
            normalPileup,
            logOddsThreshold,
            minAlignmentQuality,
            filterMultiAllelic
          )

        val foundVariant = SomaticGenotypeFilter(
          calledGenotypes,
          minTumorReadDepth,
          maxTumorReadDepth,
          minNormalReadDepth,
          minTumorAlternateReadDepth,
          logOddsThreshold,
          minVAF = minVAF,
          minLikelihood).nonEmpty

        foundVariant should be(shouldFindVariant)
    })
  }

  test("testing simple positive variants") {
    val (tumorReads, normalReads) =
      TestUtil.loadTumorNormalReads(
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
      TestUtil.loadTumorNormalReads(
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
      TestUtil.loadTumorNormalReads(
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
      TestUtil.loadTumorNormalReads(
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

  test("no indels") {
    val normalReads =
      makeReads(
        ("TCGATCGA", "8M", 0),
        ("TCGATCGA", "8M", 0),
        ("TCGATCGA", "8M", 0)
      )

    val normalPileup = Pileup(normalReads, "chr1", 2, simpleReference.getContig("chr1"))

    val tumorReads =
      makeReads(
        ("TCGGTCGA", "8M", 0),
        ("TCGGTCGA", "8M", 0),
        ("TCGGTCGA", "8M", 0)
      )

    val tumorPileup = Pileup(tumorReads, "chr1", 2, simpleReference.getContig("chr1"))

    SomaticStandard.Caller.findPotentialVariantAtLocus(tumorPileup, normalPileup, oddsThreshold = 2).size should be(0)
  }

  test("single-base deletion") {
    val normalReads =
      makeReads(
        ("TCGATCGA", "8M", 0),
        ("TCGATCGA", "8M", 0),
        ("TCGATCGA", "8M", 0)
      )

    val normalPileup = Pileup(normalReads, "chr1", 2, simpleReference.getContig("chr1"))

    val tumorReads =
      makeReads(
        ("TCGTCGA", "3M1D4M", 0),
        ("TCGTCGA", "3M1D4M", 0),
        ("TCGTCGA", "3M1D4M", 0)
      )

    val tumorPileup = Pileup(tumorReads, "chr1", 2, simpleReference.getContig("chr1"))

    val alleles = SomaticStandard.Caller.findPotentialVariantAtLocus(tumorPileup, normalPileup, oddsThreshold = 2)
    alleles.size should be(1)

    val allele = alleles(0).allele
    Bases.basesToString(allele.refBases) should be("GA")
    Bases.basesToString(allele.altBases) should be("G")
  }

  test("multiple-base deletion") {
    val normalReads =
      makeReads(
        "chr4",
        ("TCGAAGCTTCGAAGCT", "16M", 0),
        ("TCGAAGCTTCGAAGCT", "16M", 0),
        ("TCGAAGCTTCGAAGCT", "16M", 0)
      )

    val normalPileup = Pileup(normalReads, "chr4", 4, simpleReference.getContig("chr4"))

    val tumorReads =
      makeReads(
        "chr4",
        ("TCGAAAAGCT", "5M6D5M", 0),  // md tag: "5^GCTTCG5"
        ("TCGAAAAGCT", "5M6D5M", 0),
        ("TCGAAAAGCT", "5M6D5M", 0)
      )

    val tumorPileup = Pileup(tumorReads, "chr4", 4, simpleReference.getContig("chr4"))

    val alleles = SomaticStandard.Caller.findPotentialVariantAtLocus(tumorPileup, normalPileup, oddsThreshold = 2)
    alleles.size should be(1)

    val allele = alleles(0).allele
    Bases.basesToString(allele.refBases) should be("AGCTTCG")
    Bases.basesToString(allele.altBases) should be("A")
  }

  test("single-base insertion") {
    val normalReads =
      makeReads(
        ("TCGATCGA", "8M", 0),
        ("TCGATCGA", "8M", 0),
        ("TCGATCGA", "8M", 0)
      )

    val normalPileup = Pileup(normalReads, "chr1", 2, simpleReference.getContig("chr1"))

    val tumorReads =
      makeReads(
        ("TCGAGTCGA", "4M1I4M", 0),
        ("TCGAGTCGA", "4M1I4M", 0),
        ("TCGAGTCGA", "4M1I4M", 0)
      )

    val tumorPileup = Pileup(tumorReads, "chr1", 3, simpleReference.getContig("chr1"))

    val alleles = SomaticStandard.Caller.findPotentialVariantAtLocus(tumorPileup, normalPileup, oddsThreshold = 2)
    alleles.size should be(1)

    val allele = alleles(0).allele
    Bases.basesToString(allele.refBases) should be("A")
    Bases.basesToString(allele.altBases) should be("AG")
  }

  test("multiple-base insertion") {
    val normalReads =
      makeReads(
        ("TCGATCGA", "8M", 0),
        ("TCGATCGA", "8M", 0),
        ("TCGATCGA", "8M", 0)
      )

    val tumorReads =
      makeReads(
        ("TCGAGGTCTCGA", "4M4I4M", 0),
        ("TCGAGGTCTCGA", "4M4I4M", 0),
        ("TCGAGGTCTCGA", "4M4I4M", 0)
      )

    val alleles = SomaticStandard.Caller.findPotentialVariantAtLocus(
      Pileup(tumorReads, "chr1", 3, simpleReference.getContig("chr1")),
      Pileup(normalReads, "chr1", 3, simpleReference.getContig("chr1")),
      oddsThreshold = 2
    )
    alleles.size should be(1)

    val allele = alleles(0).allele
    Bases.basesToString(allele.refBases) should be("A")
    Bases.basesToString(allele.altBases) should be("AGGTC")
  }

  test("insertions and deletions") {
    /*
    idx:  01234  56    7890123456
    ref:  TCGAA  TC    GATCGATCGA
    seq:  TC  ATCTCAAAAGA  GATCGA
     */

    val normalReads =
      makeReads(
        "chr3",
        ("TCGAATCGATCGATCGA", "17M", 10),
        ("TCGAATCGATCGATCGA", "17M", 10),
        ("TCGAATCGATCGATCGA", "17M", 10)
      )

    val tumorReads =
      makeReads(
        "chr3",
        ("TCATCTCAAAAGAGATCGA", "2M2D1M2I2M4I2M2D6M", 10),  // md tag: "2^GA5^TC6""
        ("TCATCTCAAAAGAGATCGA", "2M2D1M2I2M4I2M2D6M", 10),
        ("TCATCTCAAAAGAGATCGA", "2M2D1M2I2M4I2M2D6M", 10)
      )

    def testLocus(contigName: ContigName, locus: Locus, refBases: String, altBases: String) = {
      val alleles = SomaticStandard.Caller.findPotentialVariantAtLocus(
        Pileup(tumorReads, contigName, locus, simpleReference.getContig("chr3")),
        Pileup(normalReads, contigName, locus, simpleReference.getContig("chr3")),
        oddsThreshold = 2
      )
      alleles.size should be(1)

      val allele = alleles(0).allele
      Bases.basesToString(allele.refBases) should be(refBases)
      Bases.basesToString(allele.altBases) should be(altBases)
    }

    testLocus("chr3", 11, "CGA", "C")
    testLocus("chr3", 14, "A", "ATC")
    testLocus("chr3", 16, "C", "CAAAA")
    testLocus("chr3", 18, "ATC", "A")
  }
}
