package org.hammerlab.guacamole.loci.set

import org.hammerlab.guacamole.loci.Coverage
import org.hammerlab.guacamole.loci.Coverage.PositionCoverage
import org.hammerlab.guacamole.reference.Position
import org.scalatest.{FunSuite, Matchers}

class TakeLociIteratorSuite extends FunSuite with Matchers {

  def check(input: ((String, Int), (Int, Int, Int))*)(expectedStrs: String*) = {
    val depths =
      for {
        ((contig, locus), (depth, starts, ends)) <- input
      } yield
        Position(contig, locus) -> Coverage(depth, starts, ends)

    val expected = expectedStrs.map(LociSet.apply)

    new TakeLociIterator(depths.iterator.buffered, 15).toList should be(expected)
  }

  test("simple") {
    check(
      ("chr1", 10) -> ( 1,  1,  0),
      ("chr1", 11) -> ( 6,  5,  0),
      ("chr1", 12) -> (15, 10,  1),
      ("chr1", 13) -> (10, 10, 15),
      ("chr1", 14) -> (10,  5,  5),
      ("chr1", 15) -> (11,  1,  0),
      ("chr2", 20) -> ( 1,  1,  0),
      ("chr2", 21) -> ( 4,  3,  0),
      ("chr2", 22) -> (11,  7,  0),
      ("chr2", 23) -> (14,  3,  0),
      ("chr2", 24) -> (15,  1,  0)
    )(
      "chr1:10-12",
      "chr1:12-13",
      "chr1:13-15",
      "chr1:15-16,chr2:20-22",
      "chr2:22-25"
    )
  }

  test("skip extreme depth") {
    check(
      ("chr1", 10) -> ( 1,  1,  0),
      ("chr1", 11) -> ( 6,  5,  0),
      ("chr1", 12) -> (15, 10,  1),
      ("chr1", 13) -> (16, 10,  9),
      ("chr1", 14) -> (11,  5, 10),
      ("chr1", 15) -> (12,  1,  0)
    )(
      "chr1:10-12",
      "chr1:12-13",
      "chr1:14-16"
    )
  }

  test("skip extreme depth at end") {
    check(
      ("chr1", 10) -> ( 1,  1,  0),
      ("chr1", 11) -> ( 6,  5,  0),
      ("chr1", 12) -> (15, 10,  1),
      ("chr1", 13) -> (16, 10,  9)
    )(
      "chr1:10-12",
      "chr1:12-13"
    )
  }
}
