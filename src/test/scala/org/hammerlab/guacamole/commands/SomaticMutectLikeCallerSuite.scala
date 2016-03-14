/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hammerlab.guacamole.commands

import org.bdgenomics.adam.util.PhredUtils
import org.hammerlab.guacamole.Bases
import org.hammerlab.guacamole.pileup.Pileup
import org.hammerlab.guacamole.reads.MappedRead
import org.hammerlab.guacamole.util.{ GuacFunSuite, TestUtil }
import org.scalatest.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalactic.TolerantNumerics
import sext._

class SomaticMutectLikeCallerSuite extends GuacFunSuite with Matchers with TableDrivenPropertyChecks {
  implicit val doubleEq = TolerantNumerics.tolerantDoubleEquality(1e-6f)

  def loadPileup(filename: String, referenceName: String, locus: Long = 0): Pileup = {
    val records = TestUtil.loadReads(sc, filename).mappedReads
    val localReads = records.collect
    Pileup(localReads, referenceName, locus)
  }

  /**
   * Common algorithm parameters - fixed for all tests
   */
  import SomaticMutectLike.DefaultMutectArgs._

  def testVariants(tumorReads: Seq[MappedRead], normalReads: Seq[MappedRead], positions: Array[Long], shouldFindVariant: Boolean = false) = {
    val positionsTable = Table("locus", positions: _*)
    forAll(positionsTable) {
      (locus: Long) =>
        val (tumorPileup, normalPileup) = TestUtil.loadTumorNormalPileup(tumorReads, normalReads, locus)

        val calledGenotypes = SomaticMutectLike.Caller.findPotentialVariantAtLocus(
          tumorPileup,
          normalPileup)
        val filteredCalledGenotypes = calledGenotypes.filter(call => {
          SomaticMutectLike.Caller.finalMutectDbSnpCosmicNoisyFilter(call,
            somDbSnpThreshold = somDbSnpLODThreshold, somNovelThreshold = somNovelLODThreshold) &&
            SomaticMutectLike.Caller.mutectHeuristicFiltersPreDbLookup(call)
        })
        val foundVariant = filteredCalledGenotypes.size > 0

        if (foundVariant != shouldFindVariant) {
          println(filteredCalledGenotypes.map(_.valueTreeString).headOption.getOrElse("No genotypes found..."))
        }

        foundVariant should be(shouldFindVariant)
    }
  }

  sparkTest("testing simple positive variants") {
    val (tumorReads, normalReads) = TestUtil.loadTumorNormalReads(sc, "tumor.chr20.tough.sam", "normal.chr20.tough.sam", mdFilter = true)
    val positivePositions = Array[Long](42999694, 25031215, 44061033, 45175149, 755754, 1843813,
      3555766, 3868620, 9896926, 14017900, 17054263, 35951019, 50472935, 51858471, 58201903, 7087895,
      19772181, 30430960, 32150541, 42186626, 44973412, 46814443, 52311925, 53774355, 57280858, 62262870)
    testVariants(tumorReads, normalReads, positivePositions, shouldFindVariant = true)
  }

  sparkTest("testing simple negative variants on syn1") {
    val (tumorReads, normalReads) = TestUtil.loadTumorNormalReads(sc,
      "synthetic.challenge.set1.tumor.v2.withMDTags.chr2.syn1fp.sam",
      "synthetic.challenge.set1.normal.v2.withMDTags.chr2.syn1fp.sam", mdFilter = true)
    val negativePositions = Array[Long](216094721, 3529313, 8789794, 104043280, 104175801,
      126651101, 241901237, 57270796, 120757852)
    testVariants(tumorReads, normalReads, negativePositions, shouldFindVariant = false)
  }

  sparkTest("testing complex region negative variants on syn1") {
    val (tumorReads, normalReads) = TestUtil.loadTumorNormalReads(sc,
      "synthetic.challenge.set1.tumor.v2.withMDTags.chr2.complexvar.sam",
      "synthetic.challenge.set1.normal.v2.withMDTags.chr2.complexvar.sam", mdFilter = true)
    val negativePositions = Array[Long](148487667, 134307261, 90376213, 3638733, 109347468)
    testVariants(tumorReads, normalReads, negativePositions, shouldFindVariant = false)

    val positivePositions = Array[Long](82949713, 130919744)
    testVariants(tumorReads, normalReads, positivePositions, shouldFindVariant = true)
  }

  sparkTest("difficult negative variants") {

    val (tumorReads, normalReads) = TestUtil.loadTumorNormalReads(sc, "tumor.chr20.simplefp.sam", "normal.chr20.simplefp.sam", mdFilter = true)
    val negativeVariantPositions = Array[Long](26211835, 29652479, 54495768, 13046318, 25939088)
    testVariants(tumorReads, normalReads, negativeVariantPositions, shouldFindVariant = false)
  }

  test("no indels") {
    val normalReads = Seq(
      TestUtil.makeRead("TCGATCGA", "8M", "8", 0),
      TestUtil.makeRead("TCGATCGA", "8M", "8", 0),
      TestUtil.makeRead("TCGATCGA", "8M", "8", 0)
    )
    val normalPileup = Pileup(normalReads, "chr1", 2)

    val tumorReads = Seq(
      TestUtil.makeRead("TCGGTCGA", "8M", "3G4", 0),
      TestUtil.makeRead("TCGGTCGA", "8M", "3G4", 0),
      TestUtil.makeRead("TCGGTCGA", "8M", "3G4", 0)
    )
    val tumorPileup = Pileup(tumorReads, "chr1", 2)

    SomaticMutectLike.Caller.findPotentialVariantAtLocus(tumorPileup, normalPileup).size should be(0)
  }

  test("single-base deletion") {
    val normalReads = Seq(
      TestUtil.makeRead("TCGATCGA", "8M", "8", 0),
      TestUtil.makeRead("TCGATCGA", "8M", "8", 0),
      TestUtil.makeRead("TCGATCGA", "8M", "8", 0))
    val normalPileup = Pileup(normalReads, "chr1", 2)

    val tumorReads = Seq(
      TestUtil.makeRead("TCGTCGA", "3M1D4M", "3^A4", 0),
      TestUtil.makeRead("TCGTCGA", "3M1D4M", "3^A4", 0),
      TestUtil.makeRead("TCGTCGA", "3M1D4M", "3^A4", 0))
    val tumorPileup = Pileup(tumorReads, "chr1", 2)

    val alleles = SomaticMutectLike.Caller.findPotentialVariantAtLocus(tumorPileup, normalPileup)
    alleles.size should be(1)

    val allele = alleles(0).allele
    Bases.basesToString(allele.refBases) should be("GA")
    Bases.basesToString(allele.altBases) should be("G")
  }

  test("multiple-base deletion") {
    val normalReads = Seq(
      TestUtil.makeRead("TCGAAGCTTCGAAGCT", "16M", "16", 0),
      TestUtil.makeRead("TCGAAGCTTCGAAGCT", "16M", "16", 0),
      TestUtil.makeRead("TCGAAGCTTCGAAGCT", "16M", "16", 0)
    )
    val normalPileup = Pileup(normalReads, "chr1", 4)

    val tumorReads = Seq(
      TestUtil.makeRead("TCGAAAAGCT", "5M6D5M", "5^GCTTCG5", 0),
      TestUtil.makeRead("TCGAAAAGCT", "5M6D5M", "5^GCTTCG5", 0),
      TestUtil.makeRead("TCGAAAAGCT", "5M6D5M", "5^GCTTCG5", 0)
    )
    val tumorPileup = Pileup(tumorReads, "chr1", 4)

    val alleles = SomaticMutectLike.Caller.findPotentialVariantAtLocus(tumorPileup, normalPileup)
    alleles.size should be(1)

    val allele = alleles(0).allele
    Bases.basesToString(allele.refBases) should be("AGCTTCG")
    Bases.basesToString(allele.altBases) should be("A")
  }

  test("single-base insertion") {
    val normalReads = Seq(
      TestUtil.makeRead("TCGATCGA", "8M", "8", 0),
      TestUtil.makeRead("TCGATCGA", "8M", "8", 0),
      TestUtil.makeRead("TCGATCGA", "8M", "8", 0)
    )
    val normalPileup = Pileup(normalReads, "chr1", 2)

    val tumorReads = Seq(
      TestUtil.makeRead("TCGAGTCGA", "4M1I4M", "8", 0),
      TestUtil.makeRead("TCGAGTCGA", "4M1I4M", "8", 0),
      TestUtil.makeRead("TCGAGTCGA", "4M1I4M", "8", 0)
    )
    val tumorPileup = Pileup(tumorReads, "chr1", 3)

    val alleles = SomaticMutectLike.Caller.findPotentialVariantAtLocus(tumorPileup, normalPileup)
    alleles.size should be(1)

    val allele = alleles(0).allele
    Bases.basesToString(allele.refBases) should be("A")
    Bases.basesToString(allele.altBases) should be("AG")
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

    val alleles = SomaticMutectLike.Caller.findPotentialVariantAtLocus(
      Pileup(tumorReads, "chr1", 3), Pileup(normalReads, "chr1", 3)
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

    def testLocus(referenceName: String, locus: Int, refBases: String, altBases: String) = {
      val tumorPileup = Pileup(tumorReads, referenceName, locus)
      val normalPileup = Pileup(normalReads, referenceName, locus)

      val alleles = SomaticMutectLike.Caller.findPotentialVariantAtLocus(
        Pileup(tumorReads, referenceName, locus), Pileup(normalReads, referenceName, locus)
      )
      alleles.size should be(1)

      val allele = alleles(0).allele
      Bases.basesToString(allele.refBases) should be(refBases)
      Bases.basesToString(allele.altBases) should be(altBases)
    }

    testLocus("chr1", 11, "CGA", "C")
    testLocus("chr1", 14, "A", "ATC")
    testLocus("chr1", 16, "C", "CAAAA")
    testLocus("chr1", 18, "ATC", "A")
  }

  test("Power calculations") {
    val errorQ30Alt3 = PhredUtils.phredToErrorProbability(30) / 3.0d
    val power035 = SomaticMutectLike.calculateStrandPower(50, 0.35d, errorQ30Alt3, 6.3d)
    assert(power035 === 0.9999994d)
    val power005 = SomaticMutectLike.calculateStrandPower(50, 0.05d, errorQ30Alt3, 4.3d)
    assert(power005 === 0.6068266d)
  }

}
