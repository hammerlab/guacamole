package org.hammerlab.guacamole.loci.set

import org.hammerlab.guacamole.reference.TestInterval
import org.scalatest.{FunSuite, Matchers}

class LociIteratorSuite extends FunSuite with Matchers {

  def loci(intervals: (Int, Int)*): LociIterator =
    new LociIterator(
      (for {
        (start, end) <- intervals
      } yield
        TestInterval(start, end)
      ).iterator.buffered
    )

  test("simple") {
    loci(100 -> 110).toList should be(100 until 110)
  }

  test("skipTo") {
    val it = loci(100 -> 110)
    it.skipTo(103)
    it.head should be(103)
    it.toList should be(103 until 110)
  }

  test("intervals") {
    loci(100 -> 110, 120 -> 130).toList should be((100 until 110) ++ (120 until 130))
  }
}
