package org.hammerlab.guacamole.readsets.iterator

import org.hammerlab.guacamole.reads.{ReadsUtil, TestRegion}
import org.scalatest.{FunSuite, Matchers}
import org.hammerlab.guacamole.reads.TestRegion._

class ContigsIteratorSuite extends FunSuite with Matchers with ReadsUtil {
  test("simple") {
    ContigsIterator(
      Iterator[TestRegion](
        ("chr1", 10, 20),
        ("chr1", 30, 40),
        ("chr2", 50, 60),
        ("chr3", 70, 80)
      ).buffered
    ).map(t => t._1 -> t._2.toList).toList should be(
      List(
        "chr1" ->
          List(
            TestRegion("chr1", 10, 20),
            TestRegion("chr1", 30, 40)
          ),
        "chr2" ->
          List(
            TestRegion("chr2", 50, 60)
          ),
        "chr3" ->
          List(
            TestRegion("chr3", 70, 80)
          )
      )
    )
  }

  test("partially-consumed contig") {
    val it =
      ContigsIterator(
        Iterator[TestRegion](
          ("chr1", 10, 20),
          ("chr1", 30, 40),
          ("chr2", 50, 60)
        ).buffered
      )

    val (chr1Name, chr1) = it.next()
    chr1Name should be("chr1")
    chr1.next() should be(TestRegion("chr1", 10, 20))

    // Skip ahead to chr2 before exhausting chr1.
    val (chr2Name, chr2) = it.next()
    chr2Name should be("chr2")
    chr2.toList should be(List(TestRegion("chr2", 50, 60)))
  }
}
