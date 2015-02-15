package org.hammerlab.guacamole.reads

import org.hammerlab.guacamole.{Bases, TestUtil}
import org.scalatest.{FunSuite, Matchers}

class MDTagUtilsSuite extends FunSuite with Matchers {

  test("rebuild all matching reference") {
    val sequence = "GATGATTCGA"
    val read = TestUtil.makeRead(sequence, "10M", "10")
    val reference = MDTagUtils.getReference(read, allowNBase = true)
    reference should be(Bases.stringToBases(sequence))
  }

  test("rebuild reference with mismatches ") {
    val sequence = "GATGATTCGA"
    val read = TestUtil.makeRead(sequence, "10M", "0CC8")
    val reference = MDTagUtils.getReference(read, allowNBase = true)
    reference should be(Bases.stringToBases("CCTGATTCGA"))
  }

  test("rebuild reference with indel") {
    val sequence = "GATGACCCTTCGA"
    val read = TestUtil.makeRead(sequence, "5M3I5M", "10")
    val reference = MDTagUtils.getReference(read, allowNBase = true)
    reference should be(Bases.stringToBases("GATGATTCGA"))
  }

  test("rebuild reference with deletion") {
    val sequence = "GATA"
    val read = TestUtil.makeRead(sequence, "3M6D1M", "3^GATTCG1")
    val reference = MDTagUtils.getReference(read, allowNBase = true)
    reference should be(Bases.stringToBases("GATGATTCGA"))
  }

  test("rebuild reference from multiple reads") {
    val originalReference = "AAATTGATACTCGAACGA"

    val firstRead = TestUtil.makeRead(originalReference.substring(0, 10), "10M", "10", start = 0)
    val secondRead = TestUtil.makeRead(originalReference.substring(5, 15), "10M", "10", start = 5)
    val thirdRead = TestUtil.makeRead(originalReference.substring(8, 18), "10M", "10", start = 8)

    val reference = MDTagUtils.getReference(Seq(firstRead, secondRead, thirdRead))
    Bases.basesToString(reference) should be(originalReference)
  }

  test("rebuild reference from multiple reads with mismatch") {
    val originalReference = "AAATTGATACTCGAACGA"
    val firstRead = TestUtil.makeRead(originalReference.substring(0, 10), "10M", "10", start = 0)
    val secondRead = TestUtil.makeRead("GCTACTCGAA", "10M", "1A9", start = 5)
    val thirdRead = TestUtil.makeRead(originalReference.substring(8, 18), "10M", "10", start = 8)

    val reference = MDTagUtils.getReference(Seq(firstRead, secondRead, thirdRead))
    Bases.basesToString(reference) should be(originalReference)
  }

  test("rebuild reference from multiple reads with multiple mismatches") {
    val originalReference = "AAATTGATACTCGAACGA"
    val firstRead = TestUtil.makeRead(originalReference.substring(0, 10), "10M", "10", start = 0)
    val secondRead = TestUtil.makeRead("GCGGGTACTCGAA", "10M", "1C5G2", start = 5)
    val thirdRead = TestUtil.makeRead(originalReference.substring(8, 18), "10M", "10", start = 8)

    val reference = MDTagUtils.getReference(Seq(firstRead, secondRead, thirdRead))
    Bases.basesToString(reference) should be(originalReference)
  }

  test("rebuild reference from multiple reads with insertion") {
    val originalReference = "AAATTGATACTCGAACGA"
    val firstRead = TestUtil.makeRead(originalReference.substring(0, 10), "10M", "10", start = 0)
    val secondRead = TestUtil.makeRead("GCGGGTACTCGAA", "2M3I7M", "10", start = 5)
    val thirdRead = TestUtil.makeRead(originalReference.substring(8, 18), "10M", "10", start = 8)

    val reference = MDTagUtils.getReference(Seq(firstRead, secondRead, thirdRead))
    Bases.basesToString(reference) should be(originalReference)
  }

  test("rebuild reference from multiple reads with insertion and mismatches") {
    val originalReference = "AAATTGATACTCGAACGA"
    val firstRead = TestUtil.makeRead(originalReference.substring(0, 10), "10M", "10", start = 0)
    val secondRead = TestUtil.makeRead("GAGGGTACTCGAA", "2M3I7M", "1A5G2", start = 5)
    val thirdRead = TestUtil.makeRead("ACTCGAATTA", "10M", "7CG1", start = 8)

    val reference = MDTagUtils.getReference(Seq(firstRead, secondRead, thirdRead))
    Bases.basesToString(reference) should be(originalReference)
  }

  test("rebuild reference from multiple reads with deletion") {
    val originalReference = "AAATTGATACTCGAACGA"
    val firstRead = TestUtil.makeRead(originalReference.substring(0, 10), "10M", "10", start = 0)
    val secondRead = TestUtil.makeRead("GAGAA", "2M5D3M", "2^TACTC3", start = 5)
    val thirdRead = TestUtil.makeRead(originalReference.substring(8, 18), "10M", "10", start = 8)

    val reference = MDTagUtils.getReference(Seq(firstRead, secondRead, thirdRead))
    Bases.basesToString(reference) should be(originalReference)
  }

  test("rebuild reference from multiple reads with multiple deletions") {
    val originalReference = "AAATTGATACTCGAACGA"
    val firstRead = TestUtil.makeRead(originalReference.substring(0, 10), "10M", "10", start = 0)
    val secondRead = TestUtil.makeRead("GAGAA", "2M5D3M", "2^TACTC3", start = 5)
    val thirdRead = TestUtil.makeRead("ACTCGA", "5M4D1M", "5^AACG1", start = 8)

    val reference = MDTagUtils.getReference(Seq(firstRead, secondRead, thirdRead))
    Bases.basesToString(reference) should be(originalReference)
  }

  test("rebuild reference from multiple reads with gap") {
    val originalReference = "AAATTGATACTCGAACGA"
    val firstRead = TestUtil.makeRead(originalReference.substring(0, 7), "7M", "7", start = 0)
    val secondRead = TestUtil.makeRead(originalReference.substring(11, 18), "7M", "7", start = 11)

    val reference = MDTagUtils.getReference(Seq(firstRead, secondRead))
    Bases.basesToString(reference) should be("AAATTGANNNNCGAACGA")
  }
}
