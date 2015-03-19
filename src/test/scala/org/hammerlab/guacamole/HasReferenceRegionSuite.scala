package org.hammerlab.guacamole

import org.scalatest.{ Matchers, FunSuite }

class HasReferenceRegionSuite extends FunSuite with Matchers {

  test("overlapping reads") {
    val read1 = TestUtil.makeRead("TCGATCGA", cigar = "8M", mdtag = "8", start = 1L)
    val read2 = TestUtil.makeRead("TCGATCGA", cigar = "8M", mdtag = "8", start = 5L)

    read1.overlaps(read2) should be(true)
    read2.overlaps(read1) should be(true)

  }

  test("overlapping reads, different contigs") {
    val read1 = TestUtil.makeRead("TCGATCGA", cigar = "8M", mdtag = "8", start = 1L)
    val read2 = TestUtil.makeRead("TCGATCGA", cigar = "8M", mdtag = "8", start = 5L, chr = "chr2")

    read1.overlaps(read2) should be(false)
    read2.overlaps(read1) should be(false)

  }

  test("nonoverlapping reads") {
    val read1 = TestUtil.makeRead("TCGATCGA", cigar = "8M", mdtag = "8", start = 1L)
    val read2 = TestUtil.makeRead("TCGATCGA", cigar = "8M", mdtag = "8", start = 10L)

    read1.overlaps(read2) should be(false)
    read2.overlaps(read1) should be(false)

  }

  test("overlapping reads on start") {
    val read1 = TestUtil.makeRead("TCGATCGA", cigar = "8M", mdtag = "8", start = 1L)
    val read2 = TestUtil.makeRead("TCGATCGA", cigar = "8M", mdtag = "8", start = 8L)

    read1.overlaps(read2) should be(true)
    read2.overlaps(read1) should be(true)

  }

  test("read completely covers another") {
    val read1 = TestUtil.makeRead("TCGATCGA", cigar = "8M", mdtag = "8", start = 1L)
    val read2 = TestUtil.makeRead("TCG", cigar = "3M", mdtag = "3", start = 5L)

    read1.overlaps(read2) should be(true)
    read2.overlaps(read1) should be(true)

  }

}
