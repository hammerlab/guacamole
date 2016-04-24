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
import org.hammerlab.guacamole.reference.ReferenceBroadcast
import org.hammerlab.guacamole.util.{ GuacFunSuite, TestUtil }
import org.scalatest.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalactic.TolerantNumerics
import sext._

class SomaticMutectLikeCallerSuite extends GuacFunSuite with Matchers with TableDrivenPropertyChecks {
  implicit val doubleEq = TolerantNumerics.tolerantDoubleEquality(1e-6f)

  def grch37Reference = ReferenceBroadcast(TestUtil.testDataPath("grch37.partial.fasta"), sc, partialFasta = true)

  def simpleReference = TestUtil.makeReference(sc, Seq(
    ("chr1", 0, "TCGATCGACG"),
    ("chr2", 0, "TCGAAGCTTCG"),
    ("chr3", 10, "TCGAATCGATCGATCGA"),
    ("chr4", 0, "TCGAAGCTTCGAAGCT")))

  def loadPileup(filename: String, contig: String, locus: Long = 0): Pileup = {
    val contigReference = grch37Reference.getContig(contig)
    val records = TestUtil.loadReads(sc, filename).mappedReads
    val localReads = records.collect
    Pileup(localReads, contig, locus, contigReference)
  }

  /**
   * Common algorithm parameters - fixed for all tests
   */
  import SomaticMutectLike.DefaultMutectArgs._

  def testVariants(tumorReads: Seq[MappedRead], normalReads: Seq[MappedRead], positions: Array[Long], shouldFindVariant: Boolean = false) = {
    val positionsTable = Table("locus", positions: _*)
    forAll(positionsTable) {
      (locus: Long) =>
        val (tumorPileup, normalPileup) = TestUtil.loadTumorNormalPileup(tumorReads,
          normalReads, locus, grch37Reference)

        val calledGenotypes = SomaticMutectLike.Caller.findPotentialVariantAtLocus(
          tumorPileup,
          normalPileup,
          contamFrac = 0.0)
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
    val (tumorReads, normalReads) = TestUtil.loadTumorNormalReads(sc,
      "tumor.chr20.tough.sam",
      "normal.chr20.tough.sam")
    val positivePositions = Array[Long](42999694, 25031215, 44061033, 45175149, 755754, 1843813,
      3555766, 3868620, 9896926, 14017900, 17054263, 35951019, 50472935, 51858471, 58201903, 7087895,
      19772181, 30430960, 32150541, 42186626, 44973412, 46814443, 52311925, 53774355, 57280858, 62262870)
    testVariants(tumorReads, normalReads, positivePositions, shouldFindVariant = true)
  }

  sparkTest("testing simple negative variants on syn1") {
    val (tumorReads, normalReads) = TestUtil.loadTumorNormalReads(sc,
      "synthetic.challenge.set1.tumor.v2.withMDTags.chr2.syn1fp.sam",
      "synthetic.challenge.set1.normal.v2.withMDTags.chr2.syn1fp.sam")
    val negativePositions = Array[Long](216094721, 3529313, 8789794, 104043280, 104175801,
      126651101, 241901237, 57270796, 120757852)
    testVariants(tumorReads, normalReads, negativePositions, shouldFindVariant = false)
  }

  sparkTest("testing complex region negative variants on syn1") {
    val (tumorReads, normalReads) = TestUtil.loadTumorNormalReads(sc,
      "synthetic.challenge.set1.tumor.v2.withMDTags.chr2.complexvar.sam",
      "synthetic.challenge.set1.normal.v2.withMDTags.chr2.complexvar.sam")
    val negativePositions = Array[Long](148487667, 134307261, 90376213, 3638733, 109347468)
    testVariants(tumorReads, normalReads, negativePositions, shouldFindVariant = false)

    val positivePositions = Array[Long](82949713, 130919744)
    testVariants(tumorReads, normalReads, positivePositions, shouldFindVariant = true)
  }

  sparkTest("difficult negative variants") {

    val (tumorReads, normalReads) = TestUtil.loadTumorNormalReads(
      sc,
      "tumor.chr20.simplefp.sam",
      "normal.chr20.simplefp.sam")
    val negativeVariantPositions = Array[Long](26211835, 29652479, 54495768, 13046318, 25939088)
    testVariants(tumorReads, normalReads, negativeVariantPositions, shouldFindVariant = false)
  }

  sparkTest("no indels") {
    val normalReads = Seq(
      TestUtil.makeRead("TCGATCGA", "8M", 0),
      TestUtil.makeRead("TCGATCGA", "8M", 0),
      TestUtil.makeRead("TCGATCGA", "8M", 0)
    )
    val normalPileup = Pileup(normalReads, "chr1", 2, referenceContigSequence = simpleReference.getContig("chr1"))

    val tumorReads = Seq(
      TestUtil.makeRead("TCGGTCGA", "8M", 0),
      TestUtil.makeRead("TCGGTCGA", "8M", 0),
      TestUtil.makeRead("TCGGTCGA", "8M", 0)
    )
    val tumorPileup = Pileup(tumorReads, "chr1", 2, referenceContigSequence = simpleReference.getContig("chr1"))

    SomaticMutectLike.Caller.findPotentialVariantAtLocus(tumorPileup, normalPileup,
      contamFrac = 0.0).size should be(0)
  }

  sparkTest("single-base deletion") {
    val normalReads = Seq(
      TestUtil.makeRead("TCGATCGA", "8M", 0),
      TestUtil.makeRead("TCGATCGA", "8M", 0),
      TestUtil.makeRead("TCGATCGA", "8M", 0))
    val normalPileup = Pileup(normalReads, "chr1", 2, referenceContigSequence = simpleReference.getContig("chr1"))

    val tumorReads = Seq(
      TestUtil.makeRead("TCGTCGA", "3M1D4M", 0),
      TestUtil.makeRead("TCGTCGA", "3M1D4M", 0),
      TestUtil.makeRead("TCGTCGA", "3M1D4M", 0))
    val tumorPileup = Pileup(tumorReads, "chr1", 2, referenceContigSequence = simpleReference.getContig("chr1"))

    val alleles = SomaticMutectLike.Caller.findPotentialVariantAtLocus(tumorPileup, normalPileup,
      contamFrac = 0.0)
    alleles.size should be(1)

    val allele = alleles(0).allele
    Bases.basesToString(allele.refBases) should be("GA")
    Bases.basesToString(allele.altBases) should be("G")
  }

  sparkTest("multiple-base deletion") {
    val normalReads = Seq(
      TestUtil.makeRead("TCGAAGCTTCGAAGCT", "16M", 0, "chr4"),
      TestUtil.makeRead("TCGAAGCTTCGAAGCT", "16M", 0, "chr4"),
      TestUtil.makeRead("TCGAAGCTTCGAAGCT", "16M", 0, "chr4")
    )
    //TCGAAGCTTCG
    val normalPileup = Pileup(normalReads, "chr4", 4, referenceContigSequence = simpleReference.getContig("chr4"))

    val tumorReads = Seq(
      TestUtil.makeRead("TCGAAAAGCT", "5M6D5M", 0, "chr4"), // md tag: "5^GCTTCG5"
      TestUtil.makeRead("TCGAAAAGCT", "5M6D5M", 0, "chr4"),
      TestUtil.makeRead("TCGAAAAGCT", "5M6D5M", 0, "chr4")
    )
    val tumorPileup = Pileup(tumorReads, "chr4", 4, referenceContigSequence = simpleReference.getContig("chr4"))
    val alleles = SomaticMutectLike.Caller.findPotentialVariantAtLocus(tumorPileup, normalPileup,
      contamFrac = 0.0)
    alleles.size should be(1)

    val allele = alleles(0).allele
    Bases.basesToString(allele.refBases) should be("AGCTTCG")
    Bases.basesToString(allele.altBases) should be("A")
  }

  sparkTest("single-base insertion") {
    val normalReads = Seq(
      TestUtil.makeRead("TCGATCGA", "8M", 0),
      TestUtil.makeRead("TCGATCGA", "8M", 0),
      TestUtil.makeRead("TCGATCGA", "8M", 0)
    )
    val normalPileup = Pileup(normalReads, "chr1", 2, referenceContigSequence = simpleReference.getContig("chr1"))

    val tumorReads = Seq(
      TestUtil.makeRead("TCGAGTCGA", "4M1I4M", 0),
      TestUtil.makeRead("TCGAGTCGA", "4M1I4M", 0),
      TestUtil.makeRead("TCGAGTCGA", "4M1I4M", 0)
    )
    val tumorPileup = Pileup(tumorReads, "chr1", 3, referenceContigSequence = simpleReference.getContig("chr1"))

    val alleles = SomaticMutectLike.Caller.findPotentialVariantAtLocus(tumorPileup, normalPileup,
      contamFrac = 0.0)
    alleles.size should be(1)

    val allele = alleles(0).allele
    Bases.basesToString(allele.refBases) should be("A")
    Bases.basesToString(allele.altBases) should be("AG")
  }

  sparkTest("multiple-base insertion") {
    val normalReads = Seq(
      TestUtil.makeRead("TCGATCGA", "8M", 0),
      TestUtil.makeRead("TCGATCGA", "8M", 0),
      TestUtil.makeRead("TCGATCGA", "8M", 0)
    )

    val tumorReads = Seq(
      TestUtil.makeRead("TCGAGGTCTCGA", "4M4I4M", 0),
      TestUtil.makeRead("TCGAGGTCTCGA", "4M4I4M", 0),
      TestUtil.makeRead("TCGAGGTCTCGA", "4M4I4M", 0)
    )

    val alleles = SomaticMutectLike.Caller.findPotentialVariantAtLocus(
      Pileup(tumorReads, "chr1", 3, referenceContigSequence = simpleReference.getContig("chr1")),
      Pileup(normalReads, "chr1", 3, referenceContigSequence = simpleReference.getContig("chr1")),
      contamFrac = 0.0
    )
    alleles.size should be(1)

    val allele = alleles(0).allele
    Bases.basesToString(allele.refBases) should be("A")
    Bases.basesToString(allele.altBases) should be("AGGTC")
  }

  sparkTest("insertions and deletions") {
    /*
    idx:  01234  56    7890123456
    ref:  TCGAA  TC    GATCGATCGA
    seq:  TC  ATCTCAAAAGA  GATCGA
     */

    val normalReads = Seq(
      TestUtil.makeRead("TCGAATCGATCGATCGA", "17M", 10, chr = "chr3"),
      TestUtil.makeRead("TCGAATCGATCGATCGA", "17M", 10, chr = "chr3"),
      TestUtil.makeRead("TCGAATCGATCGATCGA", "17M", 10, chr = "chr3")
    )

    val tumorReads = Seq(
      TestUtil.makeRead("TCATCTCAAAAGAGATCGA", "2M2D1M2I2M4I2M2D6M", 10, chr = "chr3"), // md tag: "2^GA5^TC6""
      TestUtil.makeRead("TCATCTCAAAAGAGATCGA", "2M2D1M2I2M4I2M2D6M", 10, chr = "chr3"),
      TestUtil.makeRead("TCATCTCAAAAGAGATCGA", "2M2D1M2I2M4I2M2D6M", 10, chr = "chr3")
    )

    def testLocus(referenceName: String, locus: Int, refBases: String, altBases: String) = {
      val tumorPileup = Pileup(tumorReads, referenceName, locus, referenceContigSequence = simpleReference.getContig("chr2"))
      val normalPileup = Pileup(normalReads, referenceName, locus, referenceContigSequence = simpleReference.getContig("chr2"))

      val alleles = SomaticStandard.Caller.findPotentialVariantAtLocus(
        Pileup(tumorReads, referenceName, locus, referenceContigSequence = simpleReference.getContig("chr3")),
        Pileup(normalReads, referenceName, locus, referenceContigSequence = simpleReference.getContig("chr3")),
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

  sparkTest("Power calculations") {
    val errorQ30Alt3 = PhredUtils.phredToErrorProbability(30) / 3.0d
    val power035 = SomaticMutectLike.calculatePowerToDetect(50, 0.35d, errorQ30Alt3, 6.3d, 0.0)
    assert(power035 === 0.9999994d)
    val power005 = SomaticMutectLike.calculatePowerToDetect(50, 0.05d, errorQ30Alt3, 4.3d, 0.0)
    assert(power005 === 0.6068266d)
    val power005contam = SomaticMutectLike.calculatePowerToDetect(50, 0.05d, errorQ30Alt3, 4.3d, 0.02)
    assert(power005contam === 0.0039135)
  }

  sparkTest("Test distance to nearest indel") {
    val tumorReads = Seq(
      TestUtil.makeRead("TCATCTCAAAAGAGATCGA", "2M2D1M2I2M4I2M2D6M", 10, chr = "chr3")
    )
    val tumorPileup11 = Pileup(tumorReads, "chr3", 11, referenceContigSequence = simpleReference.getContig("chr3")) // one position into the first match, actually carries the deletion, but technically is a match at this piont
    val tumorPileup20 = Pileup(tumorReads, "chr3", 22, referenceContigSequence = simpleReference.getContig("chr3")) // two positions into the final 6M
    val distanceInsertion11 = SomaticMutectLike.Caller.distanceToNearestReadInsertionOrDeletion(tumorPileup11.elements(0), true)
    val distanceDeletion11 = SomaticMutectLike.Caller.distanceToNearestReadInsertionOrDeletion(tumorPileup11.elements(0), false)
    val distanceInsertion20 = SomaticMutectLike.Caller.distanceToNearestReadInsertionOrDeletion(tumorPileup20.elements(0), true)
    val distanceDeletion20 = SomaticMutectLike.Caller.distanceToNearestReadInsertionOrDeletion(tumorPileup20.elements(0), false)
    assert(distanceInsertion11.getOrElse(-5) === 2)
    assert(distanceDeletion11.getOrElse(-5) === 1)
    assert(distanceInsertion20.getOrElse(-5) === 4)
    assert(distanceDeletion20.getOrElse(-5) === 2)

  }

}
