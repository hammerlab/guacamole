package org.hammerlab.guacamole.readsets.iterator

import org.hammerlab.guacamole.loci.Coverage
import org.hammerlab.guacamole.reads.TestRegion
import org.hammerlab.guacamole.readsets.{ContigLengths, ContigLengthsUtil}
import org.hammerlab.guacamole.reference.ContigIterator
import org.scalatest.{FunSuite, Matchers}

class ContigCoverageIteratorSuite
  extends FunSuite
    with Matchers {

  def check(halfWindowSize: Int,
            intervals: (Int, Int)*)(
    expectedStrs: (Int, (Int, Int))*
  ): Unit = {

    val reads =
      (for {
        (start, end) <- intervals
      } yield
        TestRegion("foo", start, end)
      ).iterator.buffered

    val expected =
      for {
        (locus, (depth, starts)) <- expectedStrs
      } yield
        locus -> Coverage(depth, starts)

    ContigCoverageIterator(halfWindowSize, ContigIterator(reads)).toList should be(expected)
  }

  test("simple") {
    check(
      halfWindowSize = 0,
      (10, 20)
    )(
      10 -> (1, 1),
      11 -> (1, 0),
      12 -> (1, 0),
      13 -> (1, 0),
      14 -> (1, 0),
      15 -> (1, 0),
      16 -> (1, 0),
      17 -> (1, 0),
      18 -> (1, 0),
      19 -> (1, 0)
    )
  }

  test("contained reads") {
    check(
      halfWindowSize = 0,
      (0, 10),
      (1,  9),
      (2,  8),
      (3, 11)
    )(
       0 -> (1, 1),
       1 -> (2, 1),
       2 -> (3, 1),
       3 -> (4, 1),
       4 -> (4, 0),
       5 -> (4, 0),
       6 -> (4, 0),
       7 -> (4, 0),
       8 -> (3, 0),
       9 -> (2, 0),
      10 -> (1, 0)
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

  test("throw on unsorted regions") {
    val reads =
      ContigIterator(
        Iterator(
          TestRegion("chr1",  0, 10),
          TestRegion("chr1",  1,  9),
          TestRegion("chr1",  2, 21),
          TestRegion("chr1",  1, 31)
        ).buffered
      )

    val it = ContigCoverageIterator(halfWindowSize = 0, reads)
    it.next() should be( 0 -> Coverage(1, 1))
    it.next() should be( 1 -> Coverage(2, 1))
    intercept[RegionsNotSortedException] {
      it.next()
    }
  }
}
