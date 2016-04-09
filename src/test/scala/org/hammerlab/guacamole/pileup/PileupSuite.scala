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

package org.hammerlab.guacamole.pileup

import org.hammerlab.guacamole.Bases
import org.hammerlab.guacamole.reads.MappedRead
import org.hammerlab.guacamole.util.TestUtil.Implicits._
import org.hammerlab.guacamole.util.TestUtil.{ assertBases, loadPileup }
import org.hammerlab.guacamole.util.{ GuacFunSuite, TestUtil }
import org.hammerlab.guacamole.variants.Allele
import org.scalatest.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

class PileupSuite extends GuacFunSuite with Matchers with TableDrivenPropertyChecks {
  // This must only be accessed from inside a spark test where SparkContext has been initialized
  def reference = TestUtil.makeReference(sc,
    Seq(
      ("chr1", 0, "NTCGATCGACG"),
      ("artificial", 0, "A" * 34 + "G" * 10 + "A" * 5 + "G" * 15 + "A" * 15 + "ACGT" * 10),
      ("chr2", 0, "AATTG"),
      ("chr3", 0, "AAATTT"),
      ("chr4", 0, "AATTGCAATTG")
    ))

  def testAdamRecords = TestUtil.loadReads(sc, "different_start_reads.sam").mappedReads.collect()

  def pileupElementFromRead(read: MappedRead, locus: Long): PileupElement = {
    PileupElement(read, locus, reference.getContig(read.referenceContig))
  }

  sparkTest("create pileup from long insert reads") {
    val reads = Seq(
      TestUtil.makeRead("TCGATCGA", "8M", 1),
      TestUtil.makeRead("TCGATCGA", "8M", 1),
      TestUtil.makeRead("TCGACCCTCGA", "4M3I4M", 1))

    val noPileup = Pileup(reads, "chr1", 0, reference.getContig("chr1")).elements
    assert(noPileup.size === 0)

    val firstPileup = Pileup(reads, "chr1", 1, reference.getContig("chr1"))
    firstPileup.elements.forall(_.isMatch) should be(true)
    firstPileup.elements.forall(_.qualityScore == 31) should be(true)

    val insertPileup = Pileup(reads, "chr1", 4, reference.getContig("chr1"))
    insertPileup.elements.exists(_.isInsertion) should be(true)
    insertPileup.elements.forall(_.qualityScore == 31) should be(true)

    insertPileup.elements(0).alignment should equal(Match('A', 31.toByte))
    insertPileup.elements(1).alignment should equal(Match('A', 31.toByte))
    insertPileup.elements(2).alignment should equal(Insertion("ACCC", Seq(31, 31, 31, 31).map(_.toByte)))
  }

  sparkTest("create pileup from long insert reads; different qualities in insertion") {
    val reads = Seq(
      TestUtil.makeRead("TCGATCGA", "8M", 1, "chr1", Some(Seq(10, 15, 20, 25, 10, 15, 20, 25))),
      TestUtil.makeRead("TCGATCGA", "8M", 1, "chr1", Some(Seq(10, 15, 20, 25, 10, 15, 20, 25))),
      TestUtil.makeRead("TCGACCCTCGA", "4M3I4M", 1, "chr1", Some(Seq(10, 15, 20, 25, 5, 5, 5, 10, 15, 20, 25))))

    val insertPileup = Pileup(reads, "chr1", 4, reference.getContig("chr1"))
    insertPileup.elements.exists(_.isInsertion) should be(true)
    insertPileup.elements.exists(_.qualityScore == 5) should be(true)

    insertPileup.elements.forall(
      _.alignment match {
        case Match(_, quality)       => quality == 25
        case Insertion(_, qualities) => qualities.sameElements(Seq(25, 5, 5, 5))
        case _                       => false
      }) should be(true)
  }

  sparkTest("create pileup from long insert reads, right after insertion") {
    val reads = Seq(
      TestUtil.makeRead("TCGATCGA", "8M", 1, "chr1", Some(Seq(10, 15, 20, 25, 10, 15, 20, 25))),
      TestUtil.makeRead("TCGATCGA", "8M", 1, "chr1", Some(Seq(10, 15, 20, 25, 10, 15, 20, 25))),
      TestUtil.makeRead("TCGACCCTCGA", "4M3I4M", 1, "chr1", Some(Seq(10, 15, 20, 25, 5, 5, 5, 10, 15, 20, 25)))
    )

    val noPileup = Pileup(reads, "chr1", 0, reference.getContig("chr1")).elements
    noPileup.size should be(0)

    val pastInsertPileup = Pileup(reads, "chr1", 5, reference.getContig("chr1"))
    pastInsertPileup.elements.foreach(_.isMatch should be(true))

    pastInsertPileup.elements.foreach(_.qualityScore should be(10))

  }

  sparkTest("create pileup from long insert reads; after insertion") {
    val reads = Seq(
      TestUtil.makeRead("TCGATCGA", "8M", 1),
      TestUtil.makeRead("TCGATCGA", "8M", 1),
      TestUtil.makeRead("TCGACCCTCGA", "4M3I4M", 1))
    val lastPileup = Pileup(reads, "chr1", 7, reference.getContig("chr1"))
    lastPileup.elements.foreach(e => assertBases(e.sequencedBases, "G"))
    lastPileup.elements.forall(_.isMatch) should be(true)
  }

  sparkTest("create pileup from long insert reads; end of read") {

    val reads = Seq(
      TestUtil.makeRead("TCGATCGA", "8M", 1, "chr1", Some(Seq(10, 15, 20, 25, 10, 15, 20, 25))),
      TestUtil.makeRead("TCGATCGA", "8M", 1, "chr1", Some(Seq(10, 15, 20, 25, 10, 15, 20, 25))),
      TestUtil.makeRead("TCGACCCTCGA", "4M3I4M", 1, "chr1", Some(Seq(10, 15, 20, 25, 5, 5, 5, 10, 15, 20, 25))))

    val lastPileup = Pileup(reads, "chr1", 8, reference.getContig("chr1"))
    lastPileup.elements.foreach(e => assertBases(e.sequencedBases, "A"))
    lastPileup.elements.forall(_.sequencedBases.headOption.exists(_ == Bases.A)) should be(true)

    lastPileup.elements.forall(_.isMatch) should be(true)
    lastPileup.elements.forall(_.qualityScore == 25) should be(true)
  }

  sparkTest("Load pileup from SAM file") {
    val pileup = loadPileup(sc, "same_start_reads.sam", locus = 0, reference = reference)
    pileup.elements.length should be(10)
  }

  sparkTest("First 60 loci should have all 10 reads") {
    val pileup = loadPileup(sc, "same_start_reads.sam", locus = 0, reference = reference)
    for (i <- 1 to 59) {
      val nextPileup = pileup.atGreaterLocus(i, Seq.empty.iterator)
      nextPileup.elements.length should be(10)
    }
  }

  sparkTest("test pileup element creation") {
    val read = TestUtil.makeRead("AATTG", "5M", 0, "chr2")
    val firstElement = pileupElementFromRead(read, 0)

    firstElement.isMatch should be(true)
    firstElement.indexWithinCigarElement should be(0L)

    val secondElement = firstElement.advanceToLocus(1L)
    secondElement.isMatch should be(true)
    secondElement.indexWithinCigarElement should be(1L)

    val thirdElement = secondElement.advanceToLocus(2L)
    thirdElement.isMatch should be(true)
    thirdElement.indexWithinCigarElement should be(2L)

  }

  sparkTest("test pileup element creation with multiple cigar elements") {
    val read = TestUtil.makeRead("AAATTT", "3M3M", 0, "chr3")

    val secondMatch = pileupElementFromRead(read, 3)
    secondMatch.isMatch should be(true)
    secondMatch.indexWithinCigarElement should be(0L)

    val secondMatchSecondElement = pileupElementFromRead(read, 4)
    secondMatchSecondElement.isMatch should be(true)
    secondMatchSecondElement.indexWithinCigarElement should be(1L)

  }

  sparkTest("insertion at contig start includes trailing base") {
    val contigStartInsertionRead = TestUtil.makeRead("AAAAAACGT", "5I4M", 0, "chr1")
    val pileup = pileupElementFromRead(contigStartInsertionRead, 0)
    pileup.alignment should equal(Insertion("AAAAAA", List(31, 31, 31, 31, 31, 31)))
  }

  sparkTest("pileup alignment at insertion cigar-element throws") {
    val contigStartInsertionRead = TestUtil.makeRead("AAAAAACGT", "5I4M", 0, "chr1")
    val pileup = PileupElement(
      read = contigStartInsertionRead,
      locus = 1,
      referenceContigSequence = reference.getContig("chr1"),
      readPosition = 0,
      cigarElementIndex = 0,
      cigarElementLocus = 1,
      indexWithinCigarElement = 0
    )
    the[InvalidCigarElementException] thrownBy pileup.alignment
  }

  sparkTest("test pileup element creation with deletion cigar elements") {
    val read = TestUtil.makeRead("AATTGAATTG", "5M1D5M", 0, "chr4")
    val firstElement = pileupElementFromRead(read, 0)

    firstElement.isMatch should be(true)
    firstElement.indexWithinCigarElement should be(0L)

    val deletionElement = firstElement.advanceToLocus(4L)
    deletionElement.alignment should equal(Deletion("GC", 1.toByte))
    deletionElement.isDeletion should be(true)
    deletionElement.indexWithinCigarElement should be(4L)

    val midDeletionElement = deletionElement.advanceToLocus(5L)
    midDeletionElement.isMidDeletion should be(true)
    midDeletionElement.indexWithinCigarElement should be(0L)

    val pastDeletionElement = midDeletionElement.advanceToLocus(6L)
    pastDeletionElement.isMatch should be(true)
    pastDeletionElement.indexWithinCigarElement should be(0L)

    val continuePastDeletionElement = pastDeletionElement.advanceToLocus(9L)
    continuePastDeletionElement.isMatch should be(true)
    continuePastDeletionElement.indexWithinCigarElement should be(3L)

  }

  sparkTest("Loci 10-19 deleted from half of the reads") {
    val pileup = loadPileup(sc, "same_start_reads.sam", locus = 0, reference = reference)
    val deletionPileup = pileup.atGreaterLocus(9, Seq.empty.iterator)
    deletionPileup.elements.map(_.alignment).count {
      case Deletion(bases, _) => {
        Bases.basesToString(bases) should equal("AAAAAAAAAAA")
        true
      }
      case _ => false
    } should be(5)
    for (i <- 10 to 19) {
      val nextPileup = pileup.atGreaterLocus(i, Seq.empty.iterator)
      nextPileup.elements.count(_.isMidDeletion) should be(5)
    }
  }

  sparkTest("Loci 60-69 have 5 reads") {
    val pileup = loadPileup(sc, "same_start_reads.sam", locus = 0, reference = reference)
    for (i <- 60 to 69) {
      val nextPileup = pileup.atGreaterLocus(i, Seq.empty.iterator)
      nextPileup.elements.length should be(5)
    }
  }

  sparkTest("Pileup.Element basic test") {
    intercept[NullPointerException] {
      val e = pileupElementFromRead(null, 42)
    }

    val reference = TestUtil.makeReference(sc,
      Seq(
        ("artificial", 0, "A" * 500)
      ))

    val decadentRead1 = testAdamRecords(0)

    // read1 starts at SAM:6 → 0-based 5
    // and has CIGAR: 29M10D31M
    // so, the length is 70
    intercept[AssertionError] {
      pileupElementFromRead(decadentRead1, 0)
    }
    intercept[AssertionError] {
      pileupElementFromRead(decadentRead1, 4)
    }
    intercept[AssertionError] {
      pileupElementFromRead(decadentRead1, 5 + 70)
    }
    val at5 = pileupElementFromRead(decadentRead1, 5)
    assert(at5 != null)
    assertBases(at5.sequencedBases, "A")
    assert(at5.sequencedBases.headOption.exists(_ == Bases.A))

    // At the end of the read:
    assert(pileupElementFromRead(decadentRead1, 74) != null)
    intercept[AssertionError] {
      pileupElementFromRead(decadentRead1, 75)
    }

    // Just before the deletion
    val deletionPileup = pileupElementFromRead(decadentRead1, 5 + 28)
    deletionPileup.alignment should equal(Deletion("AGGGGGGGGGG", 1.toByte))

    // Inside the deletion
    val at29 = pileupElementFromRead(decadentRead1, 5 + 29)
    assert(at29.sequencedBases.size === 0)
    val at38 = pileupElementFromRead(decadentRead1, 5 + 38)
    assert(at38.sequencedBases.size === 0)
    // Just after the deletion
    assertBases(pileupElementFromRead(decadentRead1, 5 + 39).sequencedBases, "A")

    // advanceToLocus is a no-op on the same locus,
    // and fails in lower loci
    forAll(Table("locus", List(5, 33, 34, 43, 44, 74): _*)) { locus =>
      val elt = pileupElementFromRead(decadentRead1, locus)
      assert(elt.advanceToLocus(locus) === elt)
      intercept[AssertionError] {
        elt.advanceToLocus(locus - 1)
      }
      intercept[AssertionError] {
        elt.advanceToLocus(75)
      }
    }

    val read3Record = testAdamRecords(2) // read3
    val read3At15 = pileupElementFromRead(read3Record, 15)
    assert(read3At15 != null)
    assertBases(read3At15.sequencedBases, "A")
    assertBases(read3At15.advanceToLocus(16).sequencedBases, "T")
    assertBases(read3At15.advanceToLocus(17).sequencedBases, "C")
    assertBases(read3At15.advanceToLocus(16).advanceToLocus(17).sequencedBases, "C")
    assertBases(read3At15.advanceToLocus(18).sequencedBases, "G")
  }

  sparkTest("Read4 has CIGAR: 10M10I10D40M. It's ACGT repeated 15 times") {
    val decadentRead4 = testAdamRecords(3)
    val read4At20 = pileupElementFromRead(decadentRead4, 20)
    assert(read4At20 != null)
    for (i <- 0 until 2) {
      assert(read4At20.advanceToLocus(20 + i * 4 + 0).sequencedBases(0) == 'A')
      assert(read4At20.advanceToLocus(20 + i * 4 + 1).sequencedBases(0) == 'C')
      assert(read4At20.advanceToLocus(20 + i * 4 + 2).sequencedBases(0) == 'G')
      assert(read4At20.advanceToLocus(20 + i * 4 + 3).sequencedBases(0) == 'T')
    }

    val read4At30 = read4At20.advanceToLocus(20 + 9)
    read4At30.isInsertion should be(true)
    (read4At30.sequencedBases: String) should equal("CGTACGTACGT")
  }

  sparkTest("Read5: ACGTACGTACGTACG, 5M4=1X5=") {
    // Read5: ACGTACGTACGTACG, 5M4=1X5=, [10; 25[
    //        MMMMM====G=====
    val decadentRead5 = testAdamRecords(4)
    val read5At10 = pileupElementFromRead(decadentRead5, 10)
    assert(read5At10 != null)
    assertBases(read5At10.advanceToLocus(10).sequencedBases, "A")
    assertBases(read5At10.advanceToLocus(14).sequencedBases, "A")
    assertBases(read5At10.advanceToLocus(18).sequencedBases, "A")
    assertBases(read5At10.advanceToLocus(19).sequencedBases, "C")
    assertBases(read5At10.advanceToLocus(20).sequencedBases, "G")
    assertBases(read5At10.advanceToLocus(21).sequencedBases, "T")
    assertBases(read5At10.advanceToLocus(22).sequencedBases, "A")
    assertBases(read5At10.advanceToLocus(24).sequencedBases, "G")
  }

  sparkTest("read6: ACGTACGTACGT 4=1N4=4S") {
    // Read6: ACGTACGTACGT 4=1N4=4S
    // one `N` and soft-clipping at the end
    val decadentRead6 = testAdamRecords(5)
    val read6At99 = pileupElementFromRead(decadentRead6, 99)

    assert(read6At99 != null)
    assertBases(read6At99.advanceToLocus(99).sequencedBases, "A")
    assertBases(read6At99.advanceToLocus(100).sequencedBases, "C")
    assertBases(read6At99.advanceToLocus(101).sequencedBases, "G")
    assertBases(read6At99.advanceToLocus(102).sequencedBases, "T")
    assertBases(read6At99.advanceToLocus(103).sequencedBases, "")
    assertBases(read6At99.advanceToLocus(104).sequencedBases, "A")
    assertBases(read6At99.advanceToLocus(107).sequencedBases, "T")
    intercept[AssertionError] {
      read6At99.advanceToLocus(49).sequencedBases
    }
  }

  sparkTest("read7: ACGTACGT 4=1N4=4H, one `N` and hard-clipping at the end") {
    val decadentRead7 = testAdamRecords(6)
    val read7At99 = pileupElementFromRead(decadentRead7, 99)
    assert(read7At99 != null)
    assertBases(read7At99.advanceToLocus(99).sequencedBases, "A")
    assertBases(read7At99.advanceToLocus(100).sequencedBases, "C")
    assertBases(read7At99.advanceToLocus(101).sequencedBases, "G")
    assertBases(read7At99.advanceToLocus(102).sequencedBases, "T")
    assertBases(read7At99.advanceToLocus(103).sequencedBases, "")
    assertBases(read7At99.advanceToLocus(104).sequencedBases, "A")
    assertBases(read7At99.advanceToLocus(107).sequencedBases, "T")
    intercept[AssertionError] {
      read7At99.advanceToLocus(49).sequencedBases
    }
  }

  sparkTest("create and advance pileup element from RNA read") {
    val reference = TestUtil.makeReference(sc, Seq(("chr1", 229538779, "A" * 1000)), 229538779 + 1000)

    val rnaRead = TestUtil.makeRead(
      sequence = "CCCCAGCCTAGGCCTTCGACACTGGGGGGCTGAGGGAAGGGGCACCTGCC",
      cigar = "7M191084N43M",
      start = 229538779,
      chr = "chr1")

    val rnaPileupElement = PileupElement(rnaRead, 229538779L, referenceContigSequence = reference.getContig("chr1"))

    // Second base
    assertBases(rnaPileupElement.advanceToLocus(229538780L).sequencedBases, "C")

    // Third base
    assertBases(rnaPileupElement.advanceToLocus(229538781L).sequencedBases, "C")

    // In intron
    assertBases(rnaPileupElement.advanceToLocus(229539779L).sequencedBases, "")

    // Last base
    assertBases(rnaPileupElement.advanceToLocus(229729912L).sequencedBases, "C")

  }

  sparkTest("create pileup from RNA reads") {
    val reference = TestUtil.makeReference(sc, Seq(("1", 229538779, "A" * 1000)), 229538779 + 1000)
    val rnaReadsPileup = loadPileup(sc, "testrna.sam", locus = 229580594, reference = reference)

    // 94 reads in the testrna.sam
    // 3 reads end at 229580707 and 1 extends further
    rnaReadsPileup.depth should be(94)

    val movedRnaReadsPileup = rnaReadsPileup.atGreaterLocus(229580706, Iterator.empty)
    movedRnaReadsPileup.depth should be(4)

    movedRnaReadsPileup.atGreaterLocus(229580707, Iterator.empty).depth should be(1)
  }

  sparkTest("pileup in the middle of a deletion") {
    val reads = Seq(
      TestUtil.makeRead("TCGAAAAGCT", "5M6D5M", 0),
      TestUtil.makeRead("TCGAAAAGCT", "5M6D5M", 0),
      TestUtil.makeRead("TCGAAAAGCT", "5M6D5M", 0)
    )
    val deletionPileup = Pileup(reads, "chr1", 4, reference.getContig("chr1"))
    val deletionAlleles = deletionPileup.distinctAlleles
    deletionAlleles.size should be(1)
    deletionAlleles(0) should be(Allele("ATCGACG", "A"))

    val midDeletionPileup = Pileup(reads, "chr1", 5, reference.getContig("chr1"))
    val midAlleles = midDeletionPileup.distinctAlleles
    midAlleles.size should be(1)
    midAlleles(0) should be(Allele("T", ""))
  }
}

