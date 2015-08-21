package org.hammerlab.guacamole.reads

import org.hammerlab.guacamole.Bases
import org.hammerlab.guacamole.util.TestUtil
import org.scalatest.{ FunSuite, Matchers }

class MDTagUtilsSuite extends FunSuite with Matchers {

  test("rebuild all matching reference") {
    val sequence = "GATGATTCGA"
    val read = TestUtil.makeRead(sequence, "10M", "10")
    val (_, reference) = MDTagUtils.getReference(read, allowNBase = true)
    reference should be(Bases.stringToBases(sequence))
  }

  test("rebuild reference with mismatches ") {
    val sequence = "GATGATTCGA"
    val read = TestUtil.makeRead(sequence, "10M", "0CC8")
    val (_, reference) = MDTagUtils.getReference(read, allowNBase = true)
    reference should be(Bases.stringToBases("CCTGATTCGA"))
  }

  test("rebuild reference with indel") {
    val sequence = "GATGACCCTTCGA"
    val read = TestUtil.makeRead(sequence, "5M3I5M", "10")
    val (_, reference) = MDTagUtils.getReference(read, allowNBase = true)
    reference should be(Bases.stringToBases("GATGATTCGA"))
  }

  test("rebuild reference with deletion") {
    val sequence = "GATA"
    val read = TestUtil.makeRead(sequence, "3M6D1M", "3^GATTCG1")
    val (_, reference) = MDTagUtils.getReference(read, allowNBase = true)
    reference should be(Bases.stringToBases("GATGATTCGA"))
  }

  test("rebuild reference from multiple reads") {
    val originalReference = "AAATTGATACTCGAACGA"

    /*
        ref: AAATTGATACTCGAACGA
         r1: AAATTGATAC
         r2:      GATACTCGAA
         r3:         ACTCGAACGA
     */

    val firstRead = TestUtil.makeRead(originalReference.substring(0, 10), "10M", "10", start = 0)
    val secondRead = TestUtil.makeRead(originalReference.substring(5, 15), "10M", "10", start = 5)
    val thirdRead = TestUtil.makeRead(originalReference.substring(8, 18), "10M", "10", start = 8)

    val reference = MDTagUtils.getReference(Seq(firstRead, secondRead, thirdRead), 0, 18)
    Bases.basesToString(reference) should be(originalReference)
  }

  test("rebuild reference from multiple reads with mismatch") {
    val originalReference = "AAATTGATACTCGAACGA"

    /*
        ref: AAATTGATACTCGAACGA
         r1: AAATTGATAC
         r2:      GCTACTCGAA
         r3:         ACTCGAACGA
     */

    val firstRead = TestUtil.makeRead(originalReference.substring(0, 10), "10M", "10", start = 0)
    val secondRead = TestUtil.makeRead("GCTACTCGAA", "10M", "1A9", start = 5)
    val thirdRead = TestUtil.makeRead(originalReference.substring(8, 18), "10M", "10", start = 8)

    val reference = MDTagUtils.getReference(Seq(firstRead, secondRead, thirdRead), 0, 18)
    Bases.basesToString(reference) should be(originalReference)
  }

  test("rebuild reference from multiple reads with multiple mismatches") {
    val originalReference = "AAATTGATACTCGAACGA"

    /*
        ref: AAATTGATACTCGAACGA
         r1: AAATTGATAC
         r2:      GCTACTCAAA
         r3:         ACTCGAACGA
     */
    val firstRead = TestUtil.makeRead(originalReference.substring(0, 10), "10M", "10", start = 0)
    val secondRead = TestUtil.makeRead("GCTACTCAAA", "10M", "1A5G2", start = 5)
    val thirdRead = TestUtil.makeRead(originalReference.substring(8, 18), "10M", "10", start = 8)

    val reference = MDTagUtils.getReference(Seq(firstRead, secondRead, thirdRead), 0, 18)
    Bases.basesToString(reference) should be(originalReference)
  }

  test("rebuild subset of reference from multiple reads with multiple mismatches") {
    val originalReference = "AAATTGATACTCGAACGA"

    /*
        ref: AAATTGATACTCGAACGA
         r1: AAATTGATAC
         r2:      GCTACTCAAA
         r3:         ACTCGAACGA
     */
    val firstRead = TestUtil.makeRead(originalReference.substring(0, 10), "10M", "10", start = 0)
    val secondRead = TestUtil.makeRead("GCTACTCAAA", "10M", "1A5G2", start = 5)
    val thirdRead = TestUtil.makeRead(originalReference.substring(8, 18), "10M", "10", start = 8)

    val reference = MDTagUtils.getReference(Seq(firstRead, secondRead, thirdRead), 5, 12)
    Bases.basesToString(reference) should be("GATACTC")
  }

  test("rebuild reference from multiple reads with insertion") {
    val originalReference = "AAATTGATACTCGAACGA"

    /*
        ref: AAATTGA   TACTCGAACGA
         r1: AAATTGA   TAC
         r2:      GAGGGTACTCGAA
         r3:            ACTCGAACGA
     */
    val firstRead = TestUtil.makeRead(originalReference.substring(0, 10), "10M", "10", start = 0)
    val secondRead = TestUtil.makeRead("GAGGGTACTCGAA", "2M3I8M", "10", start = 5)
    val thirdRead = TestUtil.makeRead(originalReference.substring(8, 18), "10M", "10", start = 8)

    val reference = MDTagUtils.getReference(Seq(firstRead, secondRead, thirdRead), 0, 18)
    Bases.basesToString(reference) should be(originalReference)
  }

  test("rebuild reference from multiple reads with insertion and mismatches") {
    val originalReference = "AAATTGATACTCGAACGA"

    /*
        ref: AAATTGA   TACTCGAACGA
         r1: AAATTGA   TAC
         r2:      GCGGGTACTCGAA
         r3:            ACTCGAATTA
     */
    val firstRead = TestUtil.makeRead(originalReference.substring(0, 10), "10M", "10", start = 0)
    val secondRead = TestUtil.makeRead("GCGGGTACTCGAA", "2M3I8M", "1A5G2", start = 5)
    val thirdRead = TestUtil.makeRead("ACTCGAATTA", "10M", "7CG1", start = 8)

    val reference = MDTagUtils.getReference(Seq(firstRead, secondRead, thirdRead), 0, 18)
    Bases.basesToString(reference) should be(originalReference)
  }

  test("rebuild reference from multiple reads with deletion") {
    /*
      ref: AAATTGATACTCGAACGA
       r1: AAATTGATAC
       r2:      GA     GAA
       r3:         ACTCGAACGA
    */
    val originalReference = "AAATTGATACTCGAACGA"
    val firstRead = TestUtil.makeRead(originalReference.substring(0, 10), "10M", "10", start = 0)
    val secondRead = TestUtil.makeRead("GAGAA", "2M5D3M", "2^TACTC3", start = 5)
    val thirdRead = TestUtil.makeRead(originalReference.substring(8, 18), "10M", "10", start = 8)

    val reference = MDTagUtils.getReference(Seq(firstRead, secondRead, thirdRead), 0, 18)
    Bases.basesToString(reference) should be(originalReference)
  }

  test("rebuild reference from multiple reads with multiple deletions") {
    /*
      ref: AAATTGATACTCGAACGA
       r1: AAATTGATAC
       r2:      GA     GAA
       r3:         ACTCG     A
    */
    val originalReference = "AAATTGATACTCGAACGA"
    val firstRead = TestUtil.makeRead(originalReference.substring(0, 10), "10M", "10", start = 0)
    val secondRead = TestUtil.makeRead("GAGAA", "2M5D3M", "2^TACTC3", start = 5)
    val thirdRead = TestUtil.makeRead("ACTCGA", "5M4D1M", "5^AACG1", start = 8)

    val reference = MDTagUtils.getReference(Seq(firstRead, secondRead, thirdRead), 0, 18)
    Bases.basesToString(reference) should be(originalReference)
  }

  test("rebuild reference from multiple reads with gap") {
    /*
      ref: AAATTGATACTCGAACGA
       r1: AAATTGA
       r2:            CGAACGA
    */
    val originalReference = "AAATTGATACTCGAACGA"
    val firstRead = TestUtil.makeRead(originalReference.substring(0, 7), "7M", "7", start = 0)
    val secondRead = TestUtil.makeRead(originalReference.substring(11, 18), "7M", "7", start = 11)

    val reference = MDTagUtils.getReference(Seq(firstRead, secondRead), 0, 18)
    Bases.basesToString(reference) should be("AAATTGANNNNCGAACGA")
  }

  test("rebuild reference with padding to start") {
    /*
      ref: AAATTGATACTCGAACGA
       r1:    TTGA
       r2:            CGAACGA
    */
    val originalReference = "AAATTGATACTCGAACGA"
    val firstRead = TestUtil.makeRead(originalReference.substring(3, 7), "4M", "4", start = 3)
    val secondRead = TestUtil.makeRead(originalReference.substring(11, 18), "7M", "7", start = 11)

    val reference = MDTagUtils.getReference(Seq(firstRead, secondRead), 0, 18)
    Bases.basesToString(reference) should be("NNNTTGANNNNCGAACGA")
  }

  test("rebuild reference with padding to end") {
    /*
      ref: AAATTGATACTCGAACGA
       r1: AAATTGA
       r2:            CGA
    */
    val originalReference = "AAATTGATACTCGAACGA"
    val firstRead = TestUtil.makeRead(originalReference.substring(0, 7), "7M", "7", start = 0)
    val secondRead = TestUtil.makeRead(originalReference.substring(11, 14), "3M", "3", start = 11)

    val reference = MDTagUtils.getReference(Seq(firstRead, secondRead), 0, 18)
    Bases.basesToString(reference) should be("AAATTGANNNNCGANNNN")
  }

  test("rebuild reference: RNA read with N CIGAR operator") {
    val rnaRead = TestUtil.makeRead(
      sequence = "CCCCAGCCTAGGCCTTCGACACTGGGGGGCTGAGGGAAGGGGCACCTGCC",
      cigar = "7M191084N43M",
      mdtag = "9T24T7G7",
      start = 229538779,
      chr = "chr1")
    val referenceLength = (rnaRead.end - rnaRead.start).toInt
    println(rnaRead.end)
    val reference = MDTagUtils.getReference(rnaRead, allowNBase = true)._2

    reference.size should be(referenceLength)
    Bases.basesToString(reference.slice(0, 7).toSeq) should be("CCCCAGC")
    (Bases.basesToString(reference.slice(referenceLength - 43, referenceLength).toSeq)
      should be("CTAGGCCTTCGACACTGGGGGGCTGAGGGAAGGGGCACCTGCC"))

  }

  test("rebuild reference") {
    val read = TestUtil.makeRead("TCGATCGA", "8M", "1A6", 1, alignmentQuality = 60)
    val reference = MDTagUtils.getReference(read, false)._2

    Bases.basesToString(reference) should be("TAGATCGA")

  }
}
