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

import org.hammerlab.guacamole.Bases
import org.hammerlab.guacamole.filters.SomaticGenotypeFilter
import org.hammerlab.guacamole.pileup.Pileup
import org.hammerlab.guacamole.reads.MappedRead
import org.hammerlab.guacamole.reference.ReferenceGenome
import org.hammerlab.guacamole.util.{GuacFunSuite, TestUtil}
import org.scalatest.prop.TableDrivenPropertyChecks

class SomaticStandardCallerSuite extends GuacFunSuite with TableDrivenPropertyChecks {

  def grch37Reference = ReferenceGenome(TestUtil.testDataPath("grch37.partial.fasta"), partialFasta = true)

  def simpleReference = TestUtil.makeReference(
    Seq(
      ("chr1", 0, "TCGATCGACG"),
      ("chr2", 0, "TCGAAGCTTCG"),
      ("chr3", 10, "TCGAATCGATCGATCGA"),
      ("chr4", 0, "TCGAAGCTTCGAAGCT")
    )
  )

  def loadPileup(filename: String, contig: String, locus: Long = 0): Pileup = {
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
    val positionsTable = Table("locus", positions: _*)
    forAll(positionsTable) {
      (locus: Long) =>
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
    }
  }

  sparkTest("testing simple positive variants") {
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

  sparkTest("testing simple negative variants on syn1") {
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

  sparkTest("testing complex region negative variants on syn1") {
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

  sparkTest("difficult negative variants") {

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

    SomaticStandard.Caller.findPotentialVariantAtLocus(tumorPileup, normalPileup, oddsThreshold = 2).size should be(0)
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

    val alleles = SomaticStandard.Caller.findPotentialVariantAtLocus(tumorPileup, normalPileup, oddsThreshold = 2)
    alleles.size should be(1)

    val allele = alleles(0).allele
    Bases.basesToString(allele.refBases) should be("GA")
    Bases.basesToString(allele.altBases) should be("G")
  }

  sparkTest("multiple-base deletion") {
    val normalReads = Seq(
      TestUtil.makeRead("TCGAAGCTTCGAAGCT", "16M", 0, chr = "chr4"),
      TestUtil.makeRead("TCGAAGCTTCGAAGCT", "16M", 0, chr = "chr4"),
      TestUtil.makeRead("TCGAAGCTTCGAAGCT", "16M", 0, chr = "chr4")
    )
    val normalPileup = Pileup(normalReads, "chr4", 4, referenceContigSequence = simpleReference.getContig("chr4"))

    val tumorReads = Seq(
      TestUtil.makeRead("TCGAAAAGCT", "5M6D5M", 0, chr = "chr4"), // md tag: "5^GCTTCG5"
      TestUtil.makeRead("TCGAAAAGCT", "5M6D5M", 0, chr = "chr4"),
      TestUtil.makeRead("TCGAAAAGCT", "5M6D5M", 0, chr = "chr4")
    )
    val tumorPileup = Pileup(tumorReads, "chr4", 4, referenceContigSequence = simpleReference.getContig("chr4"))

    val alleles = SomaticStandard.Caller.findPotentialVariantAtLocus(tumorPileup, normalPileup, oddsThreshold = 2)
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

    val alleles = SomaticStandard.Caller.findPotentialVariantAtLocus(tumorPileup, normalPileup, oddsThreshold = 2)
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

    val alleles = SomaticStandard.Caller.findPotentialVariantAtLocus(
      Pileup(tumorReads, "chr1", 3, referenceContigSequence = simpleReference.getContig("chr1")),
      Pileup(normalReads, "chr1", 3, referenceContigSequence = simpleReference.getContig("chr1")),
      oddsThreshold = 2
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
}
