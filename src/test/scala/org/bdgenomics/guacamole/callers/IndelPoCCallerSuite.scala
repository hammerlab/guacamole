package org.bdgenomics.guacamole.callers

import org.bdgenomics.guacamole.TestUtil
import org.bdgenomics.guacamole.TestUtil.SparkFunSuite
import org.bdgenomics.guacamole.pileup.Pileup
import org.scalatest.Matchers

class IndelPoCCallerSuite extends SparkFunSuite with Matchers {
  test("no indels") {
    val normalReads = Seq(
      TestUtil.makeRead("TCGATCGA", "8M", "8", 0),
      TestUtil.makeRead("TCGATCGA", "8M", "8", 0),
      TestUtil.makeRead("TCGATCGA", "8M", "8", 0)
    )
    val normalPileup = Pileup(normalReads, 2)

    val tumorReads = Seq(
      TestUtil.makeRead("TCGGTCGA", "8M", "3G4", 0),
      TestUtil.makeRead("TCGGTCGA", "8M", "3G4", 0),
      TestUtil.makeRead("TCGGTCGA", "8M", "3G4", 0)
    )
    val tumorPileup = Pileup(tumorReads, 2)

    IndelPoCCaller.callSimpleIndelsAtLocus(tumorPileup, normalPileup).size should be(0)
  }

  test("single-base deletion") {
    val normalReads = Seq(
      TestUtil.makeRead("TCGATCGA", "8M", "8", 0),
      TestUtil.makeRead("TCGATCGA", "8M", "8", 0),
      TestUtil.makeRead("TCGATCGA", "8M", "8", 0))
    val normalPileup = Pileup(normalReads, 2)

    val tumorReads = Seq(
      TestUtil.makeRead("TCGTCGA", "3M1D4M", "3^A4", 0),
      TestUtil.makeRead("TCGTCGA", "3M1D4M", "3^A4", 0),
      TestUtil.makeRead("TCGTCGA", "3M1D4M", "3^A4", 0))
    val tumorPileup = Pileup(tumorReads, 2)

    val genotypes = IndelPoCCaller.callSimpleIndelsAtLocus(tumorPileup, normalPileup)
    genotypes.size should be(1)

    val genotype = genotypes(0)
    val variant = genotype.getVariant
    variant.getReferenceAllele should be("GA")
    variant.getAlternateAllele should be("G")
  }

  test("multiple-base deletion") {
    val normalReads = Seq(
      TestUtil.makeRead("TCGAAGCTTCGAAGCT", "16M", "16", 0),
      TestUtil.makeRead("TCGAAGCTTCGAAGCT", "16M", "16", 0),
      TestUtil.makeRead("TCGAAGCTTCGAAGCT", "16M", "16", 0)
    )
    val normalPileup = Pileup(normalReads, 4)

    val tumorReads = Seq(
      TestUtil.makeRead("TCGAAAAGCT", "5M6D5M", "5^GCTTCG5", 0),
      TestUtil.makeRead("TCGAAAAGCT", "5M6D5M", "5^GCTTCG5", 0),
      TestUtil.makeRead("TCGAAAAGCT", "5M6D5M", "5^GCTTCG5", 0)
    )
    val tumorPileup = Pileup(tumorReads, 4)

    val genotypes = IndelPoCCaller.callSimpleIndelsAtLocus(tumorPileup, normalPileup)
    genotypes.size should be(1)

    val genotype = genotypes(0)
    val variant = genotype.getVariant
    variant.getReferenceAllele should be("AGCTTCG")
    variant.getAlternateAllele should be("A")
  }

  test("single-base insertion") {
    val normalReads = Seq(
      TestUtil.makeRead("TCGATCGA", "8M", "8", 0),
      TestUtil.makeRead("TCGATCGA", "8M", "8", 0),
      TestUtil.makeRead("TCGATCGA", "8M", "8", 0)
    )
    val normalPileup = Pileup(normalReads, 2)

    val tumorReads = Seq(
      TestUtil.makeRead("TCGAGTCGA", "4M1I4M", "8", 0),
      TestUtil.makeRead("TCGAGTCGA", "4M1I4M", "8", 0),
      TestUtil.makeRead("TCGAGTCGA", "4M1I4M", "8", 0)
    )
    val tumorPileup = Pileup(tumorReads, 3)

    val genotypes = IndelPoCCaller.callSimpleIndelsAtLocus(tumorPileup, normalPileup)
    genotypes.size should be(1)

    val genotype = genotypes(0)
    val variant = genotype.getVariant
    variant.getReferenceAllele should be("A")
    variant.getAlternateAllele should be("AG")
  }

  test("multiple-base insertion") {
    val normalReads = Seq(
      TestUtil.makeRead("TCGATCGA", "8M", "8", 0),
      TestUtil.makeRead("TCGATCGA", "8M", "8", 0),
      TestUtil.makeRead("TCGATCGA", "8M", "8", 0)
    )

    val tumorReads = Seq(
      TestUtil.makeRead("TCGAGGTCTCGA", "4M4I4M", "8", 0),
      TestUtil.makeRead("TCGAGGTCTCGA", "4M4I4M", "8", 0),
      TestUtil.makeRead("TCGAGGTCTCGA", "4M4I4M", "8", 0)
    )

    val genotypes = IndelPoCCaller.callSimpleIndelsAtLocus(Pileup(tumorReads, 3), Pileup(normalReads, 3))
    genotypes.size should be(1)

    val genotype = genotypes(0)
    val variant = genotype.getVariant
    variant.getReferenceAllele should be("A")
    variant.getAlternateAllele should be("AGGTC")
  }

  test("insertions and deletions") {
    /*
    idx:  01234  56    7890123456
    ref:  TCGAA  TC    GATCGATCGA
    seq:  TC  ATCTCAAAAGA  GATCGA
     */

    val normalReads = Seq(
      TestUtil.makeRead("TCGAATCGATCGATCGA", "17M", "17", 10),
      TestUtil.makeRead("TCGAATCGATCGATCGA", "17M", "17", 10),
      TestUtil.makeRead("TCGAATCGATCGATCGA", "17M", "17", 10)
    )

    val tumorReads = Seq(
      TestUtil.makeRead("TCATCTCAAAAGAGATCGA", "2M2D1M2I2M4I2M2D6M", "2^GA5^TC6", 10),
      TestUtil.makeRead("TCATCTCAAAAGAGATCGA", "2M2D1M2I2M4I2M2D6M", "2^GA5^TC6", 10),
      TestUtil.makeRead("TCATCTCAAAAGAGATCGA", "2M2D1M2I2M4I2M2D6M", "2^GA5^TC6", 10)
    )

    def testLocus(locus: Int, refBases: String, altBases: String) = {
      val tumorPileup = Pileup(tumorReads, locus)
      val normalPileup = Pileup(normalReads, locus)
      val genotypes = IndelPoCCaller.callSimpleIndelsAtLocus(tumorPileup, normalPileup)
      genotypes.size should be(1)

      val genotype = genotypes(0)
      val variant = genotype.getVariant
      variant.getReferenceAllele should be(refBases)
      variant.getAlternateAllele should be(altBases)
    }

    testLocus(11, "CGA", "C")
    testLocus(14, "A", "ATC")
    testLocus(16, "C", "CAAAA")
    testLocus(18, "ATC", "A")
  }
}
