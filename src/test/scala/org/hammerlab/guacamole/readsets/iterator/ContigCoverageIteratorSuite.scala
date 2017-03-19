package org.hammerlab.guacamole.readsets.iterator

import org.hammerlab.genomics.loci.iterator.LociIterator
import org.hammerlab.genomics.reference.{ ContigIterator, Interval, Region }
import org.hammerlab.guacamole.loci.Coverage
import org.hammerlab.test.Suite
import org.hammerlab.genomics.reference.test.{ ClearContigNames, LociConversions }

class ContigCoverageIteratorSuite
  extends Suite
    with ClearContigNames
    with LociConversions {

  def check(halfWindowSize: Int,
            intervals: (Int, Int)*)(
    expectedStrs: (Int, (Int, Int))*
  ): Unit = {

    val reads =
      (for {
        (start, end) ← intervals
      } yield
        Region("foo", start, end)
      )
      .iterator
      .buffered

    val expected =
      for {
        (locus, (depth, starts)) ← expectedStrs
      } yield
        locus → Coverage(depth, starts)

    ContigCoverageIterator(halfWindowSize, ContigIterator(reads)).toSeq should ===(expected)
  }

  test("simple") {
    check(
      halfWindowSize = 0,
      (10, 20)
    )(
      10 → (1, 1),
      11 → (1, 0),
      12 → (1, 0),
      13 → (1, 0),
      14 → (1, 0),
      15 → (1, 0),
      16 → (1, 0),
      17 → (1, 0),
      18 → (1, 0),
      19 → (1, 0)
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
       0 → (1, 1),
       1 → (2, 1),
       2 → (3, 1),
       3 → (4, 1),
       4 → (4, 0),
       5 → (4, 0),
       6 → (4, 0),
       7 → (4, 0),
       8 → (3, 0),
       9 → (2, 0),
      10 → (1, 0)
    )
  }

  def reads =
    ContigIterator(
      Iterator(
        Region("chr1", 10, 20),
        Region("chr1", 11, 21),
        Region("chr1", 11, 21),
        Region("chr1", 30, 40)
      ).buffered
    )

  test("skips") {
    val it = ContigCoverageIterator(halfWindowSize = 1, reads)

    it.next() should ===( 9 → Coverage(1, 1))
    it.next() should ===(10 → Coverage(3, 2))
    it.skipTo(15)
    it.next() should ===(15 → Coverage(3, 0))
    it.next() should ===(16 → Coverage(3, 0))
    it.skipTo(21)
    it.next() should ===(21 → Coverage(2, 0))
    it.skipTo(25)
    it.next() should ===(29 → Coverage(1, 1))
    it.next() should ===(30 → Coverage(1, 0))
    it.skipTo(39)
    it.next() should ===(39 → Coverage(1, 0))
    it.skipTo(45)
    it.hasNext should be(false)
  }

  test("skips+starts") {
    val it = ContigCoverageIterator(halfWindowSize = 1, reads)

    it.skipTo(15)
    it.next() should ===(15 → Coverage(3, 3))
    it.skipTo(35)
    it.next() should ===(35 → Coverage(1, 1))
  }

  test("skip completely over read") {
    val it = ContigCoverageIterator(halfWindowSize = 1, reads)

    it.skipTo(21)
    it.next() should ===(21 → Coverage(2, 2))
  }

  test("load read then skip over it") {
    val it = ContigCoverageIterator(halfWindowSize = 1, reads)

    it.hasNext
    it.skipTo(21)
    it.next() should ===(21 → Coverage(2, 2))
  }

  test("skip completely over reads") {
    val it = ContigCoverageIterator(halfWindowSize = 1, reads)

    it.skipTo(25)
    it.next() should ===(29 → Coverage(1, 1))
  }

  test("intersection") {
    val lociIterator = new LociIterator(Iterator(Interval(12, 15)).buffered)
    val it = ContigCoverageIterator(halfWindowSize = 1, reads).intersect(lociIterator)

    it.toSeq should ===(
      Seq(
        12 → Coverage(3, 3),
        13 → Coverage(3, 0),
        14 → Coverage(3, 0)
      )
    )
  }

  test("throw on unsorted regions") {
    val reads =
      ContigIterator(
        Iterator(
          Region("chr1",  0, 10),
          Region("chr1",  1,  9),
          Region("chr1",  2, 21),
          Region("chr1",  1, 31)
        ).buffered
      )

    val it = ContigCoverageIterator(halfWindowSize = 0, reads)
    it.next() should ===( 0 → Coverage(1, 1))
    it.next() should ===( 1 → Coverage(2, 1))
    intercept[RegionsNotSortedException] {
      it.next()
    }
  }
}
