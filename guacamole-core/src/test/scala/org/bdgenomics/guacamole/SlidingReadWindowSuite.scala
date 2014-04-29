package org.bdgenomics.guacamole

import org.scalatest.FunSuite
import org.bdgenomics.adam.avro.{ADAMRecord, ADAMContig}
import org.scalatest.matchers.ShouldMatchers._

class SlidingReadWindowSuite extends FunSuite {

  def makeRead(sequence: String,
                cigar: String,
                mdtag: String,
                start: Long = 1,
                chr: String = "chr1"): ADAMRecord = {

    val contig = ADAMContig.newBuilder()
      .setContigName(chr)
      .build()

    ADAMRecord.newBuilder()
      .setReadName("read")
      .setStart(start)
      .setReadMapped(true)
      .setCigar(cigar)
      .setSequence(sequence)
      .setMapq(60)
      .setQual(sequence.map(x => 'F').toString)
      .setMismatchingPositions(mdtag)
      .setRecordGroupSample("sample")
      .setContig(contig)
      .build()
  }

  test("test sliding read window, duplicate reads") {

    val reads = Seq(makeRead("TCGATCGA", "8M", "8", 1), makeRead("TCGATCGA", "8M", "8", 1), makeRead("TCGATCGA", "8M", "8", 1))
    val window = SlidingReadWindow(2, reads.iterator)
    window.setCurrentLocus(0)
    assert(window.currentReads.size === 3)

  }

  test("test sliding read window, diff contigs") {

    val reads = Seq(makeRead("TCGATCGA", "8M", "8", 1, "chr1"), makeRead("TCGATCGA", "8M", "8", 1, "chr2"), makeRead("TCGATCGA", "8M", "8", 1, "chr3"))
    val window = SlidingReadWindow(2, reads.iterator)
    evaluating { window.setCurrentLocus(0) } should produce [IllegalArgumentException]

  }

  test("test sliding read window, offset reads") {

    val reads = Seq(makeRead("TCGATCGA", "8M", "8", 1), makeRead("TCGATCGA", "8M", "8", 4), makeRead("TCGATCGA", "8M", "8", 8))
    val window = SlidingReadWindow(2, reads.iterator)

    window.setCurrentLocus(0)
    assert(window.currentReads.size === 1)

    window.setCurrentLocus(4)
    assert(window.currentReads.size === 2)

  }

}
