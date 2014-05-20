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

package org.bdgenomics.guacamole.pileup

import org.bdgenomics.adam.avro.ADAMRecord
import org.scalatest.matchers.ShouldMatchers
import org.bdgenomics.adam.rich.DecadentRead
import org.bdgenomics.adam.rdd.ADAMContext._
import org.apache.spark.rdd.RDD
import org.bdgenomics.guacamole.TestUtil

class PileupSuite extends TestUtil.SparkFunSuite with ShouldMatchers {

  def loadADAMRecords(filename: String): RDD[ADAMRecord] = {
    /* grab the path to the SAM file we've stashed in the resources subdirectory */
    val path = ClassLoader.getSystemClassLoader.getResource(filename).getFile
    assert(sc != null)
    assert(sc.hadoopConfiguration != null)
    sc.adamLoad(path)
  }

  //lazy so that this is only accessed from inside a spark test where SparkContext has been initialized
  lazy val testAdamRecords = loadADAMRecords("different_start_reads.sam").collect()

  def loadPileup(filename: String, locus: Long = 0): Pileup = {
    val records = loadADAMRecords(filename)
    val localReads = records.collect.map(DecadentRead(_))
    Pileup(localReads, locus)
  }

  test("create pileup from long insert reads") {
    val reads = Seq(
      TestUtil.makeDecadentRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeDecadentRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeDecadentRead("TCGACCCTCGA", "4M3I4M", "8", 1))

    val noPileup = Pileup(reads, 0).elements
    assert(noPileup.size === 0)

    val firstPileup = Pileup(reads, 1)
    firstPileup.elements.forall(_.isMatch) should be(true)
    firstPileup.elements.forall(_.qualityScore == 31) should be(true)

    val insertPileup = Pileup(reads, 4)
    insertPileup.elements.exists(_.isInsertion) should be(true)
    insertPileup.elements.forall(_.qualityScore == 31) should be(true)

    insertPileup.elements.forall(
      _.alignment match {
        case Match(_, quality)       => quality == 31
        case Insertion(_, qualities) => qualities.sameElements(Array(31, 31, 31, 31))
        case _                       => false
      }) should be(true)
  }

  test("create pileup from long insert reads; different qualities in insertion") {
    val reads = Seq(
      TestUtil.makeDecadentRead("TCGATCGA", "8M", "8", 1, "chr1", Some(Array(10, 15, 20, 25, 10, 15, 20, 25))),
      TestUtil.makeDecadentRead("TCGATCGA", "8M", "8", 1, "chr1", Some(Array(10, 15, 20, 25, 10, 15, 20, 25))),
      TestUtil.makeDecadentRead("TCGACCCTCGA", "4M3I4M", "8", 1, "chr1", Some(Array(10, 15, 20, 25, 5, 5, 5, 10, 15, 20, 25))))

    val insertPileup = Pileup(reads, 4)
    insertPileup.elements.exists(_.isInsertion) should be(true)
    insertPileup.elements.exists(_.qualityScore == 5) should be(true)

    insertPileup.elements.forall(
      _.alignment match {
        case Match(_, quality)       => quality == 25
        case Insertion(_, qualities) => qualities.sameElements(Array(25, 5, 5, 5))
        case _                       => false
      }) should be(true)
  }

  test("create pileup from long insert reads; right after insertion") {
    val reads = Seq(
      TestUtil.makeDecadentRead("TCGATCGA", "8M", "8", 1, "chr1", Some(Array(10, 15, 20, 25, 10, 15, 20, 25))),
      TestUtil.makeDecadentRead("TCGATCGA", "8M", "8", 1, "chr1", Some(Array(10, 15, 20, 25, 10, 15, 20, 25))),
      TestUtil.makeDecadentRead("TCGACCCTCGA", "4M3I4M", "8", 1, "chr1", Some(Array(10, 15, 20, 25, 5, 5, 5, 10, 15, 20, 25))))

    val noPileup = Pileup(reads, 0).elements
    noPileup.size should be(0)

    val pastInsertPileup = Pileup(reads, 5)
    pastInsertPileup.elements.forall(_.isMatch) should be(true)

    pastInsertPileup.elements.forall(_.qualityScore == 10) should be(true)

  }

  test("create pileup from long insert reads; after insertion") {
    val reads = Seq(
      TestUtil.makeDecadentRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeDecadentRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeDecadentRead("TCGACCCTCGA", "4M3I4M", "8", 1))
    val lastPileup = Pileup(reads, 7)
    lastPileup.elements.forall(_.sequencedBases == "G") should be(true)

    lastPileup.elements.forall(_.isMatch) should be(true)
  }

  test("create pileup from long insert reads; end of read") {

    val reads = Seq(
      TestUtil.makeDecadentRead("TCGATCGA", "8M", "8", 1, "chr1", Some(Array(10, 15, 20, 25, 10, 15, 20, 25))),
      TestUtil.makeDecadentRead("TCGATCGA", "8M", "8", 1, "chr1", Some(Array(10, 15, 20, 25, 10, 15, 20, 25))),
      TestUtil.makeDecadentRead("TCGACCCTCGA", "4M3I4M", "8", 1, "chr1", Some(Array(10, 15, 20, 25, 5, 5, 5, 10, 15, 20, 25))))

    val lastPileup = Pileup(reads, 8)
    lastPileup.elements.forall(_.sequencedBases == "A") should be(true)
    lastPileup.elements.forall(_.sequencedSingleBase == 'A') should be(true)

    lastPileup.elements.forall(_.isMatch) should be(true)
    lastPileup.elements.forall(_.qualityScore == 25) should be(true)
  }

  sparkTest("Load pileup from SAM file") {
    val pileup = loadPileup("same_start_reads.sam", 0)
    pileup.elements.length should be(10)
  }

  sparkTest("First 60 loci should have all 10 reads") {
    val pileup = loadPileup("same_start_reads.sam", 0)
    for (i <- 1 to 59) {
      val nextPileup = pileup.atGreaterLocus(i, Seq.empty.iterator)
      nextPileup.elements.length should be(10)
    }
  }

  test("test pileup element creation") {
    val read = TestUtil.makeDecadentRead("AATTG", "5M", "5", 0, "chr1")
    val firstElement = PileupElement(read, 0)

    firstElement.isMatch should be(true)
    firstElement.indexInCigarElements should be(0L)
    firstElement.indexWithinCigarElement should be(0L)

    val secondElement = firstElement.elementAtGreaterLocus(1L)
    secondElement.isMatch should be(true)
    secondElement.indexInCigarElements should be(0L)
    secondElement.indexWithinCigarElement should be(1L)

    val thirdElement = secondElement.elementAtGreaterLocus(2L)
    thirdElement.isMatch should be(true)
    thirdElement.indexInCigarElements should be(0L)
    thirdElement.indexWithinCigarElement should be(2L)

  }

  test("test pileup element creation with multiple cigar elements") {
    val read = TestUtil.makeDecadentRead("AAATTT", "3M3M", "6", 0, "chr1")

    val secondMatch = PileupElement(read, 3)
    secondMatch.isMatch should be(true)
    secondMatch.indexInCigarElements should be(1L)
    secondMatch.indexWithinCigarElement should be(0L)

    val secondMatchSecondElement = PileupElement(read, 4)
    secondMatchSecondElement.isMatch should be(true)
    secondMatchSecondElement.indexInCigarElements should be(1L)
    secondMatchSecondElement.indexWithinCigarElement should be(1L)

  }

  test("test pileup element creation with deletion cigar elements") {
    val read = TestUtil.makeDecadentRead("AATTGAATTG", "5M1D5M", "5^C5", 0, "chr1")
    val firstElement = PileupElement(read, 0)

    firstElement.isMatch should be(true)
    firstElement.indexInCigarElements should be(0L)
    firstElement.indexWithinCigarElement should be(0L)

    val matchElement = firstElement.elementAtGreaterLocus(4L)
    matchElement.isMatch should be(true)
    matchElement.indexInCigarElements should be(0L)
    matchElement.indexWithinCigarElement should be(4L)

    val deletionElement = matchElement.elementAtGreaterLocus(5L)
    deletionElement.isDeletion should be(true)
    deletionElement.indexInCigarElements should be(1L)
    deletionElement.indexWithinCigarElement should be(0L)

    val pastDeletionElement = matchElement.elementAtGreaterLocus(6L)
    pastDeletionElement.isMatch should be(true)
    pastDeletionElement.indexInCigarElements should be(2L)
    pastDeletionElement.indexWithinCigarElement should be(0L)

    val continuePastDeletionElement = pastDeletionElement.elementAtGreaterLocus(9L)
    continuePastDeletionElement.isMatch should be(true)
    continuePastDeletionElement.indexInCigarElements should be(2L)
    continuePastDeletionElement.indexWithinCigarElement should be(3L)

  }

  sparkTest("Loci 10-19 deleted from half of the reads") {
    val pileup = loadPileup("same_start_reads.sam", 0)
    for (i <- 10 to 19) {
      val nextPileup = pileup.atGreaterLocus(i, Seq.empty.iterator)
      nextPileup.elements.filter(_.isDeletion).length should be(5)
    }
  }

  sparkTest("Loci 60-69 have 5 reads") {
    val pileup = loadPileup("same_start_reads.sam", 0)
    for (i <- 60 to 69) {
      val nextPileup = pileup.atGreaterLocus(i, Seq.empty.iterator)
      nextPileup.elements.length should be(5)
    }
  }

  sparkTest("Pileup.Element basic test") {
    intercept[NullPointerException] {
      val e = PileupElement(null, 42)
    }

    val read1Record = testAdamRecords(0)
    val decadentRead1 = new DecadentRead(read1Record)

    // read1 starts at SAM:6 → 0-based 5
    // and has CIGAR: 29M10D31M
    // so, the length is 70
    intercept[AssertionError] { PileupElement(decadentRead1, 0) }
    intercept[AssertionError] { PileupElement(decadentRead1, 4) }
    intercept[AssertionError] { PileupElement(decadentRead1, 5 + 70) }
    val at5 = PileupElement(decadentRead1, 5)
    assert(at5 != null)
    assert(at5.sequencedBases == "A")
    assert(at5.sequencedSingleBase == 'A')

    // At the end of the read:
    assert(PileupElement(decadentRead1, 74) != null)
    intercept[AssertionError] { PileupElement(decadentRead1, 75) }

    // Just before the deletion
    val at28 = PileupElement(decadentRead1, 5 + 28)
    assert(at28.sequencedBases === "A")
    // Inside the deletion
    val at29 = PileupElement(decadentRead1, 5 + 29)
    assert(at29.sequencedBases === "")
    val at38 = PileupElement(decadentRead1, 5 + 38)
    assert(at38.sequencedBases === "")
    // Just after the deletion
    val at39 = PileupElement(decadentRead1, 5 + 39)
    assert(at39.sequencedBases === "A")

    //  `read2` has an insertion: 5M5I34M10D16M
    val read2Record = testAdamRecords(1) // read2
    val decadentRead2 = new DecadentRead(read2Record)
    val read2At10 = PileupElement(decadentRead2, 10)
    assert(read2At10 != null)
    assert(read2At10.sequencedBases === "A")
    // right after the insert
    val read2At20 = PileupElement(decadentRead2, 20)
    assert(read2At20.sequencedBases === "A")

    // elementAtGreaterLocus is a no-op on the same locus, 
    // and fails in lower loci
    val loci = Seq(5, 33, 34, 43, 44, 74)
    loci.map({ l =>
      val elt = PileupElement(decadentRead1, l)
      assert(elt.elementAtGreaterLocus(l) === elt)
      intercept[AssertionError] { elt.elementAtGreaterLocus(l - 1) }
      intercept[AssertionError] { elt.elementAtGreaterLocus(75) }
    })

    val read3Record = testAdamRecords(2) // read3
    val decadentRead3 = new DecadentRead(read3Record)
    val read3At15 = PileupElement(decadentRead3, 15)
    assert(read3At15 != null)
    assert(read3At15.sequencedBases == "A")
    assert(read3At15.elementAtGreaterLocus(16).sequencedBases == "T")
    assert(read3At15.elementAtGreaterLocus(17).sequencedBases == "C")
    assert(read3At15.elementAtGreaterLocus(16).elementAtGreaterLocus(17).sequencedBases == "C")
    assert(read3At15.elementAtGreaterLocus(18).sequencedBases == "G")
  }

  test("Read4 has CIGAR: 10M10I10D40M; ACGT repeated 15 times") {
    // Read4 has CIGAR: 10M10I10D40M
    // It's ACGT repeated 15 times
    val decadentRead4 = new DecadentRead(testAdamRecords(3))
    val read4At20 = PileupElement(decadentRead4, 20)
    assert(read4At20 != null)
    for (i <- 0 until 2) {
      assert(read4At20.elementAtGreaterLocus(20 + i * 4 + 0).sequencedBases(0) == 'A')
      assert(read4At20.elementAtGreaterLocus(20 + i * 4 + 1).sequencedBases(0) == 'C')
      assert(read4At20.elementAtGreaterLocus(20 + i * 4 + 2).sequencedBases(0) == 'G')
      assert(read4At20.elementAtGreaterLocus(20 + i * 4 + 3).sequencedBases(0) == 'T')
    }
  }

  test("Read5: ACGTACGTACGTACG, 5M4=1X5=") {

    // Read5: ACGTACGTACGTACG, 5M4=1X5=, [10; 25[
    //        MMMMM====G=====
    val decadentRead5 = new DecadentRead(testAdamRecords(4))
    val read5At10 = PileupElement(decadentRead5, 10)
    assert(read5At10 != null)
    assert(read5At10.elementAtGreaterLocus(10).sequencedBases === "A")
    assert(read5At10.elementAtGreaterLocus(14).sequencedBases === "A")
    assert(read5At10.elementAtGreaterLocus(18).sequencedBases === "A")
    assert(read5At10.elementAtGreaterLocus(19).sequencedBases === "C")
    assert(read5At10.elementAtGreaterLocus(20).sequencedBases === "G")
    assert(read5At10.elementAtGreaterLocus(21).sequencedBases === "T")
    assert(read5At10.elementAtGreaterLocus(22).sequencedBases === "A")
    assert(read5At10.elementAtGreaterLocus(24).sequencedBases === "G")
  }

  test("read6: ACGTACGTACGT 4=1N4=4S") {
    // Read6: ACGTACGTACGT 4=1N4=4S
    // one `N` and soft-clipping at the end
    val decadentRead6 = new DecadentRead(testAdamRecords(5))
    val read6At40 = PileupElement(decadentRead6, 40)
    assert(read6At40 != null)
    assert(read6At40.elementAtGreaterLocus(40).sequencedBases === "A")
    assert(read6At40.elementAtGreaterLocus(41).sequencedBases === "C")
    assert(read6At40.elementAtGreaterLocus(42).sequencedBases === "G")
    assert(read6At40.elementAtGreaterLocus(43).sequencedBases === "T")
    assert(read6At40.elementAtGreaterLocus(44).sequencedBases === "")
    assert(read6At40.elementAtGreaterLocus(45).sequencedBases === "A")
    assert(read6At40.elementAtGreaterLocus(48).sequencedBases === "T")
    // The code uses RichADAMRecord.end which treats hard and soft-clipping
    // identically, so we cannot access, the rest of the read after the soft
    // clipping (which could have been dealt with as a “Deletion”).
    intercept[AssertionError] {
      read6At40.elementAtGreaterLocus(49).sequencedBases
    }
  }

  test("read7: ACGTACGT 4=1N4=4H, one `N` and hard-clipping at the end") {
    val decadentRead7 = new DecadentRead(testAdamRecords(6))
    val read7At40 = PileupElement(decadentRead7, 40)
    assert(read7At40 != null)
    assert(read7At40.elementAtGreaterLocus(40).sequencedBases === "A")
    assert(read7At40.elementAtGreaterLocus(41).sequencedBases === "C")
    assert(read7At40.elementAtGreaterLocus(42).sequencedBases === "G")
    assert(read7At40.elementAtGreaterLocus(43).sequencedBases === "T")
    assert(read7At40.elementAtGreaterLocus(44).sequencedBases === "")
    assert(read7At40.elementAtGreaterLocus(45).sequencedBases === "A")
    assert(read7At40.elementAtGreaterLocus(48).sequencedBases === "T")
    intercept[AssertionError] {
      read7At40.elementAtGreaterLocus(49).sequencedBases
    }
  }

  test("Read8: ACGTACGT 4=1P4=") {

    // Read8: ACGTACGT 4=1P4=
    // one `P`, a silent deletion (i.e. a deletion from a reference with a
    // virtual insertion)
    // 4=1P4= should be equivalent to 8=
    val decadentRead8 = new DecadentRead(testAdamRecords(7))
    val read8At40 = PileupElement(decadentRead8, 40)
    assert(read8At40 != null)
    assert(read8At40.elementAtGreaterLocus(40).sequencedBases === "A")
    assert(read8At40.elementAtGreaterLocus(41).sequencedBases === "C")
    assert(read8At40.elementAtGreaterLocus(42).sequencedBases === "G")
    assert(read8At40.elementAtGreaterLocus(43).sequencedBases === "T")
    assert(read8At40.elementAtGreaterLocus(44).sequencedBases === "A")
    assert(read8At40.elementAtGreaterLocus(45).sequencedBases === "C")
    assert(read8At40.elementAtGreaterLocus(46).sequencedBases === "G")
    assert(read8At40.elementAtGreaterLocus(47).sequencedBases === "T")
    intercept[AssertionError] {
      read8At40.elementAtGreaterLocus(48).sequencedBases
    }
  }

}

