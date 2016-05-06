package org.hammerlab.guacamole.reference

import org.hammerlab.guacamole.util.TestUtil
import org.scalatest.{FunSuite, Matchers}

class RegionSuite extends FunSuite with Matchers {

  test("overlapping reads") {
    val read1 = TestUtil.makeRead("TCGATCGA", cigar = "8M", start = 1L)
    val read2 = TestUtil.makeRead("TCGATCGA", cigar = "8M", start = 5L)

    read1.overlaps(read2) should be(true)
    read2.overlaps(read1) should be(true)

  }

  test("overlapping reads, different contigs") {
    val read1 = TestUtil.makeRead("TCGATCGA", cigar = "8M", start = 1L)
    val read2 = TestUtil.makeRead("TCGATCGA", cigar = "8M", start = 5L, chr = "chr2")

    read1.overlaps(read2) should be(false)
    read2.overlaps(read1) should be(false)

  }

  test("nonoverlapping reads") {
    val read1 = TestUtil.makeRead("TCGATCGA", cigar = "8M", start = 1L)
    val read2 = TestUtil.makeRead("TCGATCGA", cigar = "8M", start = 10L)

    read1.overlaps(read2) should be(false)
    read2.overlaps(read1) should be(false)

  }

  test("overlapping reads on start") {
    val read1 = TestUtil.makeRead("TCGATCGA", cigar = "8M", start = 1L)
    val read2 = TestUtil.makeRead("TCGATCGA", cigar = "8M", start = 8L)

    read1.overlaps(read2) should be(true)
    read2.overlaps(read1) should be(true)

  }

  test("read completely covers another") {
    val read1 = TestUtil.makeRead("TCGATCGA", cigar = "8M", start = 1L)
    val read2 = TestUtil.makeRead("TCG", cigar = "3M", start = 5L)

    read1.overlaps(read2) should be(true)
    read2.overlaps(read1) should be(true)
  }
}
