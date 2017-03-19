package org.hammerlab.guacamole.pileup

import org.hammerlab.genomics.bases.Base.{ A, C, G, T }
import org.hammerlab.genomics.reads.{ MappedRead, ReadsUtil }
import org.hammerlab.genomics.reference.test.LenientContigNameConversions
import org.hammerlab.genomics.reference.{ ContigName, Locus, PermissiveRegistrar }
import org.hammerlab.guacamole.reference.ReferenceUtil
import org.hammerlab.guacamole.util.GuacFunSuite
import org.hammerlab.guacamole.variants.Allele
import org.scalatest.prop.TableDrivenPropertyChecks

class PileupSuite
  extends GuacFunSuite
    with TableDrivenPropertyChecks
    with ReadsUtil
    with Util
    with LenientContigNameConversions
    with ReferenceUtil {

  register(PermissiveRegistrar)

  override implicit def contigNameFactory = ContigName.Normalization.Lenient

  override lazy val reference =
    makeReference(
      ("chr1", 0, "CTCGATCGACG"),
      ("1", 229538778, "A" * 191135),
      ("artificial", 0, "A" * 34 + "G" * 10 + "A" * 5 + "G" * 15 + "A" * 15 + "ACGT" * 10),
      ("chr2", 0, "AATTG"),
      ("chr3", 0, "AAATTT"),
      ("chr4", 0, "AATTGCAATTG")
    )

  def testAdamRecords = loadReadsRDD(sc, "different_start_reads.sam").mappedReads.collect()

  def pileupElementFromRead(read: MappedRead, locus: Locus): PileupElement =
    PileupElement(read, locus, reference.getContig(read.contigName))

  test("create pileup from long insert reads") {
    val reads =
      makeReads(
        ("TCGATCGA", "8M", 1),
        ("TCGATCGA", "8M", 1),
        ("TCGACCCTCGA", "4M3I4M", 1)
      )

    val firstPileup = makePileup(reads, "chr1", 1)
    firstPileup.elements.forall(_.isMatch) should be(true)
    firstPileup.elements.forall(_.qualityScore == 31) should be(true)

    val insertPileup = makePileup(reads, "chr1", 4)
    insertPileup.elements.exists(_.isInsertion) should be(true)
    insertPileup.elements.forall(_.qualityScore == 31) should be(true)

    insertPileup.elements(0).alignment should be(Match(A, 31))
    insertPileup.elements(1).alignment should be(Match(A, 31))
    insertPileup.elements(2).alignment should be(Insertion("ACCC", Seq(31, 31, 31, 31)))
  }

  test("create pileup from long insert reads; different qualities in insertion") {
    val reads =
      Seq(
        makeRead("TCGATCGA", "8M", 1, "chr1", Seq(10, 15, 20, 25, 10, 15, 20, 25)),
        makeRead("TCGATCGA", "8M", 1, "chr1", Seq(10, 15, 20, 25, 10, 15, 20, 25)),
        makeRead("TCGACCCTCGA", "4M3I4M", 1, "chr1", Seq(10, 15, 20, 25, 5, 5, 5, 10, 15, 20, 25))
      )

    val insertPileup = makePileup(reads, "chr1", 4)
    insertPileup.elements.exists(_.isInsertion) should be(true)
    insertPileup.elements.exists(_.qualityScore == 5) should be(true)

    insertPileup.elements.foreach(
      _.alignment match {
        case Match(_, quality)       ⇒ quality should be(25)
        case Insertion(_, qualities) ⇒ qualities should be(Seq(25, 5, 5, 5))
        case alignment               ⇒ fail(s"Unexpected Alignment: $alignment")
      }
    )
  }

  test("create pileup from long insert reads, right after insertion") {
    val reads =
      Seq(
        makeRead("TCGATCGA", "8M", 1, "chr1", Seq(10, 15, 20, 25, 10, 15, 20, 25)),
        makeRead("TCGATCGA", "8M", 1, "chr1", Seq(10, 15, 20, 25, 10, 15, 20, 25)),
        makeRead("TCGACCCTCGA", "4M3I4M", 1, "chr1", Seq(10, 15, 20, 25, 5, 5, 5, 10, 15, 20, 25))
      )

    val pastInsertPileup = makePileup(reads, "chr1", 5)
    pastInsertPileup.elements.foreach(_.isMatch should be(true))
    pastInsertPileup.elements.foreach(_.qualityScore should be(10))
  }

  test("create pileup from long insert reads; after insertion") {
    val reads =
      makeReads(
        ("TCGATCGA", "8M", 1),
        ("TCGATCGA", "8M", 1),
        ("TCGACCCTCGA", "4M3I4M", 1)
      )

    val lastPileup = makePileup(reads, "chr1", 7)
    lastPileup.elements.foreach(e ⇒ assert(e.sequencedBases === G))
    lastPileup.elements.forall(_.isMatch) should be(true)
  }

  test("create pileup from long insert reads; end of read") {

    val reads =
      Seq(
        makeRead("TCGATCGA", "8M", 1, "chr1", Seq(10, 15, 20, 25, 10, 15, 20, 25)),
        makeRead("TCGATCGA", "8M", 1, "chr1", Seq(10, 15, 20, 25, 10, 15, 20, 25)),
        makeRead("TCGACCCTCGA", "4M3I4M", 1, "chr1", Seq(10, 15, 20, 25, 5, 5, 5, 10, 15, 20, 25))
      )

    val lastPileup = makePileup(reads, "chr1", 8)
    lastPileup.elements.foreach(e ⇒ assert(e.sequencedBases === A))
    lastPileup.elements.forall(_.sequencedBases.headOption.exists(_ === A)) should be(true)

    lastPileup.elements.forall(_.isMatch) should be(true)
    lastPileup.elements.forall(_.qualityScore == 25) should be(true)
  }

  test("Load pileup from SAM file") {
    val pileup = loadPileup(sc, "same_start_reads.sam", locus = 0)
    pileup.elements.length should be(10)
  }

  test("First 60 loci should have all 10 reads") {
    val pileup = loadPileup(sc, "same_start_reads.sam", locus = 0)
    for (i ← 1 to 59) {
      val nextPileup = pileup.atGreaterLocus(i, Seq.empty.iterator)
      nextPileup.elements.length should be(10)
    }
  }

  test("test pileup element creation") {
    val read = makeRead("AATTG", "5M", 0, "chr2")
    val firstElement = pileupElementFromRead(read, 0)

    firstElement.isMatch should be(true)
    firstElement.indexWithinCigarElement should be(0L)

    val secondElement = firstElement.advanceToLocus(1)
    secondElement.isMatch should be(true)
    secondElement.indexWithinCigarElement should be(1)

    val thirdElement = secondElement.advanceToLocus(2)
    thirdElement.isMatch should be(true)
    thirdElement.indexWithinCigarElement should be(2)

  }

  test("test pileup element creation with multiple cigar elements") {
    val read = makeRead("AAATTT", "3M3M", 0, "chr3")

    val secondMatch = pileupElementFromRead(read, 3)
    secondMatch.isMatch should be(true)
    secondMatch.indexWithinCigarElement should be(0L)

    val secondMatchSecondElement = pileupElementFromRead(read, 4)
    secondMatchSecondElement.isMatch should be(true)
    secondMatchSecondElement.indexWithinCigarElement should be(1L)

  }

  test("insertion at contig start includes trailing base") {
    val contigStartInsertionRead = makeRead("AAAAAACGT", "5I4M", 0, "chr1")
    val pileup = pileupElementFromRead(contigStartInsertionRead, 0)
    pileup.alignment should be(Insertion("AAAAAA", List(31, 31, 31, 31, 31, 31)))
  }

  test("pileup alignment at insertion cigar-element throws") {
    val contigStartInsertionRead = makeRead("AAAAAACGT", "5I4M", 0, "chr1")
    val pileup = PileupElement(
      read = contigStartInsertionRead,
      locus = 1,
      contigSequence = reference.getContig("chr1"),
      readPosition = 0,
      cigarElementIndex = 0,
      cigarElementLocus = 1,
      indexWithinCigarElement = 0
    )
    the[InvalidCigarElementException] thrownBy pileup.alignment
  }

  test("test pileup element creation with deletion cigar elements") {
    val read = makeRead("AATTGAATTG", "5M1D5M", 0, "chr4")
    val firstElement = pileupElementFromRead(read, 0)

    firstElement.isMatch should be(true)
    firstElement.indexWithinCigarElement should be(0L)

    val deletionElement = firstElement.advanceToLocus(4)
    deletionElement.alignment should be(Deletion("GC", 1))
    deletionElement.isDeletion should be(true)
    deletionElement.indexWithinCigarElement should be(4)

    val midDeletionElement = deletionElement.advanceToLocus(5)
    midDeletionElement.isMidDeletion should be(true)
    midDeletionElement.indexWithinCigarElement should be(0)

    val pastDeletionElement = midDeletionElement.advanceToLocus(6)
    pastDeletionElement.isMatch should be(true)
    pastDeletionElement.indexWithinCigarElement should be(0)

    val continuePastDeletionElement = pastDeletionElement.advanceToLocus(9)
    continuePastDeletionElement.isMatch should be(true)
    continuePastDeletionElement.indexWithinCigarElement should be(3)

  }

  test("Loci 10-19 deleted from half of the reads") {
    val pileup = loadPileup(sc, "same_start_reads.sam", locus = 0)
    val deletionPileup = pileup.atGreaterLocus(9, Seq.empty.iterator)

    deletionPileup.elements.map(_.alignment).count {
      case Deletion(bases, _) ⇒
        bases should ===("AAAAAAAAAAA")
        true
      case _ ⇒ false
    } should be(5)

    for (i ← 10 to 19) {
      val nextPileup = pileup.atGreaterLocus(i, Seq.empty.iterator)
      nextPileup.elements.count(_.isMidDeletion) should be(5)
    }
  }

  test("Loci 60-69 have 5 reads") {
    val pileup = loadPileup(sc, "same_start_reads.sam", locus = 0)
    for (i ← 60 to 69) {
      val nextPileup = pileup.atGreaterLocus(i, Seq.empty.iterator)
      nextPileup.elements.length should be(5)
    }
  }

  test("Pileup.apply throws on non-overlapping reads") {
    val read = makeRead("AATTGAATTG", "5M1D5M", 1, "chr4")

    intercept[AssertionError] {
      Pileup(Seq(read), "sample", "chr4", 0, reference.getContig("chr4"))
    }

    intercept[AssertionError] {
      Pileup(Seq(read), "sample", "chr4", 12, reference.getContig("chr4"))
    }

    intercept[AssertionError] {
      Pileup(Seq(read), "sample", "chr5", 1, reference.getContig("chr4"))
    }
  }

  test("Pileup.Element basic test") {
    intercept[NullPointerException] {
      pileupElementFromRead(null, 42)
    }

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
    assert(at5.sequencedBases === A)

    // At the end of the read:
    assert(pileupElementFromRead(decadentRead1, 74) != null)
    intercept[AssertionError] {
      pileupElementFromRead(decadentRead1, 75)
    }

    // Just before the deletion
    val deletionPileup = pileupElementFromRead(decadentRead1, 5 + 28)
    deletionPileup.alignment should be(Deletion("AGGGGGGGGGG", 1))

    // Inside the deletion
    val at29 = pileupElementFromRead(decadentRead1, 5 + 29)
    assert(at29.sequencedBases.size === 0)
    val at38 = pileupElementFromRead(decadentRead1, 5 + 38)
    assert(at38.sequencedBases.size === 0)
    // Just after the deletion
    assert(pileupElementFromRead(decadentRead1, 5 + 39).sequencedBases === A)

    // advanceToLocus is a no-op on the same locus,
    // and fails in lower loci
    forAll(Table("locus", List(5, 33, 34, 43, 44, 74): _*)) { locus ⇒
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
    assert(read3At15.sequencedBases === A)
    assert(read3At15.advanceToLocus(16).sequencedBases === T)
    assert(read3At15.advanceToLocus(17).sequencedBases === C)
    assert(read3At15.advanceToLocus(16).advanceToLocus(17).sequencedBases === C)
    assert(read3At15.advanceToLocus(18).sequencedBases === G)
  }

  test("Read4 has CIGAR: 10M10I10D40M. It's ACGT repeated 15 times") {
    val decadentRead4 = testAdamRecords(3)
    val read4At20 = pileupElementFromRead(decadentRead4, 20)
    assert(read4At20 != null)
    for (i ← 0 until 2) {
      assert(read4At20.advanceToLocus(20 + i * 4 + 0).sequencedBases(0) === A)
      assert(read4At20.advanceToLocus(20 + i * 4 + 1).sequencedBases(0) === C)
      assert(read4At20.advanceToLocus(20 + i * 4 + 2).sequencedBases(0) === G)
      assert(read4At20.advanceToLocus(20 + i * 4 + 3).sequencedBases(0) === T)
    }

    val read4At30 = read4At20.advanceToLocus(20 + 9)
    read4At30.isInsertion should be(true)
    read4At30.sequencedBases should ===("CGTACGTACGT")
  }

  test("Read5: ACGTACGTACGTACG, 5M4=1X5=") {
    // Read5: ACGTACGTACGTACG, 5M4=1X5=, [10; 25[
    //        MMMMM====G=====
    val decadentRead5 = testAdamRecords(4)
    val read5At10 = pileupElementFromRead(decadentRead5, 10)
    assert(read5At10 != null)
    assert(read5At10.advanceToLocus(10).sequencedBases === A)
    assert(read5At10.advanceToLocus(14).sequencedBases === A)
    assert(read5At10.advanceToLocus(18).sequencedBases === A)
    assert(read5At10.advanceToLocus(19).sequencedBases === C)
    assert(read5At10.advanceToLocus(20).sequencedBases === G)
    assert(read5At10.advanceToLocus(21).sequencedBases === T)
    assert(read5At10.advanceToLocus(22).sequencedBases === A)
    assert(read5At10.advanceToLocus(24).sequencedBases === G)
  }

  test("read6: ACGTACGTACGT 4=1N4=4S") {
    // Read6: ACGTACGTACGT 4=1N4=4S
    // one `N` and soft-clipping at the end
    val decadentRead6 = testAdamRecords(5)
    val read6At99 = pileupElementFromRead(decadentRead6, 99)

    assert(read6At99 != null)
    assert(read6At99.advanceToLocus(99).sequencedBases === A)
    assert(read6At99.advanceToLocus(100).sequencedBases === C)
    assert(read6At99.advanceToLocus(101).sequencedBases === G)
    assert(read6At99.advanceToLocus(102).sequencedBases === T)
    assert(read6At99.advanceToLocus(103).sequencedBases === "")
    assert(read6At99.advanceToLocus(104).sequencedBases === A)
    assert(read6At99.advanceToLocus(107).sequencedBases === T)
    intercept[AssertionError] {
      read6At99.advanceToLocus(49).sequencedBases
    }
  }

  test("read7: ACGTACGT 4=1N4=4H, one `N` and hard-clipping at the end") {
    val decadentRead7 = testAdamRecords(6)
    val read7At99 = pileupElementFromRead(decadentRead7, 99)
    assert(read7At99 != null)
    assert(read7At99.advanceToLocus(99).sequencedBases === A)
    assert(read7At99.advanceToLocus(100).sequencedBases === C)
    assert(read7At99.advanceToLocus(101).sequencedBases === G)
    assert(read7At99.advanceToLocus(102).sequencedBases === T)
    assert(read7At99.advanceToLocus(103).sequencedBases === "")
    assert(read7At99.advanceToLocus(104).sequencedBases === A)
    assert(read7At99.advanceToLocus(107).sequencedBases === T)
    intercept[AssertionError] {
      read7At99.advanceToLocus(49).sequencedBases
    }
  }

  test("create2 and advance pileup element from RNA read") {
    val start = 229538779
    val rnaRead =
      makeRead(
        sequence = "ACGTAGCCTAGGCCTTCGACACTGGGGGGCTGAGGGAAGGGGCACCTGTC",
        cigar = "7M" + "1000N" + "43M",  // spans 1050 bases of reference
        start = start,
        chr = "1"
      )

    val rnaPileupElement = PileupElement(rnaRead, start, reference.getContig("1"))

    // Second base
    assert(rnaPileupElement.advanceToLocus(start + 1).sequencedBases === C)

    // Third base
    assert(rnaPileupElement.advanceToLocus(start + 2).sequencedBases === G)

    // In intron
    assert(rnaPileupElement.advanceToLocus(start + 100).sequencedBases === "")

    // Last base
    assert(rnaPileupElement.advanceToLocus(start + 1049).sequencedBases === C)
  }

  test("create2 pileup from RNA reads") {
    val rnaReadsPileup = loadPileup(sc, "testrna.sam", locus = 229580594)

    // 94 reads in the testrna.sam
    // 3 reads end at 229580707 and 1 extends further
    rnaReadsPileup.depth should be(94)

    val movedRnaReadsPileup = rnaReadsPileup.atGreaterLocus(229580706, Iterator.empty)
    movedRnaReadsPileup.depth should be(4)

    movedRnaReadsPileup.atGreaterLocus(229580707, Iterator.empty).depth should be(1)
  }

  test("pileup in the middle of a deletion") {
    val reads =
      makeReads(
        ("TCGAAAAGCT", "5M6D5M", 0),
        ("TCGAAAAGCT", "5M6D5M", 0),
        ("TCGAAAAGCT", "5M6D5M", 0)
      )

    val deletionPileup = makePileup(reads, "chr1", 4)
    val deletionAlleles = deletionPileup.distinctAlleles
    deletionAlleles.size should be(1)
    deletionAlleles(0) should be(Allele("ATCGACG", A))

    val midDeletionPileup = makePileup(reads, "chr1", 5)
    val midAlleles = midDeletionPileup.distinctAlleles
    midAlleles.size should be(1)
    midAlleles(0) should be(Allele(T, ""))
  }
}

