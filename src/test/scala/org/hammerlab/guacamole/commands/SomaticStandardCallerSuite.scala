package org.hammerlab.guacamole.commands

import org.hammerlab.guacamole.pileup.{Util => PileupUtil}
import org.hammerlab.guacamole.reads.ReadsUtil
import org.hammerlab.guacamole.reference.{ContigName, Locus, ReferenceUtil}
import org.hammerlab.guacamole.util.{Bases, GuacFunSuite, TestUtil}

class SomaticStandardCallerSuite
  extends GuacFunSuite
    with ReadsUtil
    with PileupUtil
    with ReferenceUtil {

  override lazy val reference =
    makeReference(
      sc,
      Seq(
        ("chr1", 0, "TCGATCGACG"),
        ("chr2", 0, "TCGAAGCTTCG"),
        ("chr3", 10, "TCGAATCGATCGATCGA"),
        ("chr4", 0, "TCGAAGCTTCGAAGCT")
      )
    )

  test("no indels") {
    val normalReads =
      makeReads(
        ("TCGATCGA", "8M", 0),
        ("TCGATCGA", "8M", 0),
        ("TCGATCGA", "8M", 0)
      )

    val normalPileup = makePileup(normalReads, "chr1", 2)

    val tumorReads =
      makeReads(
        ("TCGGTCGA", "8M", 0),
        ("TCGGTCGA", "8M", 0),
        ("TCGGTCGA", "8M", 0)
      )

    val tumorPileup = makePileup(tumorReads, "chr1", 2)

    SomaticStandard.Caller.findPotentialVariantAtLocus(tumorPileup, normalPileup, oddsThreshold = 2).size should be(0)
  }

  test("single-base deletion") {
    val normalReads =
      makeReads(
        ("TCGATCGA", "8M", 0),
        ("TCGATCGA", "8M", 0),
        ("TCGATCGA", "8M", 0)
      )

    val normalPileup = makePileup(normalReads, "chr1", 2)

    val tumorReads =
      makeReads(
        ("TCGTCGA", "3M1D4M", 0),
        ("TCGTCGA", "3M1D4M", 0),
        ("TCGTCGA", "3M1D4M", 0)
      )

    val tumorPileup = makePileup(tumorReads, "chr1", 2)

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

    val normalPileup = makePileup(normalReads, "chr4", 4)

    val tumorReads =
      makeReads(
        "chr4",
        ("TCGAAAAGCT", "5M6D5M", 0),  // md tag: "5^GCTTCG5"
        ("TCGAAAAGCT", "5M6D5M", 0),
        ("TCGAAAAGCT", "5M6D5M", 0)
      )

    val tumorPileup = makePileup(tumorReads, "chr4", 4)

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

    val normalPileup = makePileup(normalReads, "chr1", 2)

    val tumorReads =
      makeReads(
        ("TCGAGTCGA", "4M1I4M", 0),
        ("TCGAGTCGA", "4M1I4M", 0),
        ("TCGAGTCGA", "4M1I4M", 0)
      )

    val tumorPileup = makePileup(tumorReads, "chr1", 3)

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
      makePileup(tumorReads, "chr1", 3),
      makePileup(normalReads, "chr1", 3),
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
        makePileup(tumorReads, contigName, locus),
        makePileup(normalReads, contigName, locus),
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

