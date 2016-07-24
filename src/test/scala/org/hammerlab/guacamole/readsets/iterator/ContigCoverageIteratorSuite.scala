package org.hammerlab.guacamole.readsets.iterator

import org.hammerlab.guacamole.loci.Coverage
import org.hammerlab.guacamole.reads.TestRegion
import org.hammerlab.guacamole.readsets.{ContigLengths, ContigLengthsUtil}
import org.hammerlab.guacamole.reference.ContigIterator
import org.scalatest.{FunSuite, Matchers}

class ContigCoverageIteratorSuite extends FunSuite with Matchers with ContigLengthsUtil {

  def check(contig: String,
            halfWindowSize: Int,
            intervals: (Int, Int)*)(
    expectedStrs: ((String, Int), (Int, Int))*
  ): Unit = {

    val reads =
      (for {
        (start, end) <- intervals
      } yield
        TestRegion(contig, start, end)
      ).iterator.buffered

    val contigLengths: ContigLengths = makeContigLengths("chr1" -> 100, "chr2" -> 200)

    val expected =
      for {
        ((contig, locus), (depth, starts)) <- expectedStrs
      } yield
        locus -> Coverage(depth, starts)

    ContigCoverageIterator(halfWindowSize, ContigIterator(reads)).toList should be(expected)
  }

  test("simple") {
    check(
      "chr1",
      halfWindowSize = 0,
      (10, 20)
    )(
      ("chr1", 10) -> (1, 1),
      ("chr1", 11) -> (1, 0),
      ("chr1", 12) -> (1, 0),
      ("chr1", 13) -> (1, 0),
      ("chr1", 14) -> (1, 0),
      ("chr1", 15) -> (1, 0),
      ("chr1", 16) -> (1, 0),
      ("chr1", 17) -> (1, 0),
      ("chr1", 18) -> (1, 0),
      ("chr1", 19) -> (1, 0)
    )
  }

  test("skips") {
    val reads =
      ContigIterator(
        Iterator(
          TestRegion("chr1", 10, 20),
          TestRegion("chr1", 11, 21),
          TestRegion("chr1", 11, 21),
          TestRegion("chr1", 30, 40)
        ).buffered
      )

    val it = ContigCoverageIterator(halfWindowSize = 1, reads)

    it.next() should be( 9 -> Coverage(1, 1))
    it.next() should be(10 -> Coverage(3, 2))
    it.skipTo(15)
    it.next() should be(15 -> Coverage(3, 0))
    it.next() should be(16 -> Coverage(3, 0))
    it.skipTo(21)
    it.next() should be(21 -> Coverage(2, 0))
    it.skipTo(25)
    it.next() should be(29 -> Coverage(1, 1))
    it.next() should be(30 -> Coverage(1, 0))
    it.skipTo(39)
    it.next() should be(39 -> Coverage(1, 0))
    it.skipTo(45)
    it.hasNext should be(false)
  }
}
