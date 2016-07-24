package org.hammerlab.guacamole.loci.set

import org.hammerlab.guacamole.loci.Coverage
import org.hammerlab.guacamole.reference.Position
import org.scalatest.{FunSuite, Matchers}

class TakeLociIteratorSuite extends FunSuite with Matchers {

  def check(input: ((String, Int), (Int, Int))*)(expectedStrs: String*) = {
    val depths =
      for {
        ((contig, locus), (depth, starts)) <- input
      } yield
        (Position(contig, locus) -> Coverage(depth, starts))

    val expected = expectedStrs.map(LociSet.apply)

    new TakeLociIterator(depths.iterator.buffered, 15).toList should be(expected)
  }

  test("simple") {
    check(
      ("chr1", 10) -> ( 1,  1),
      ("chr1", 11) -> ( 6,  5),
      ("chr1", 12) -> (15, 10),
      ("chr1", 13) -> (10, 10),
      ("chr1", 14) -> (10,  5),
      ("chr1", 15) -> (11,  1),
      ("chr2", 20) -> ( 1,  1),
      ("chr2", 21) -> ( 4,  3),
      ("chr2", 22) -> (11,  7),
      ("chr2", 23) -> (14,  3),
      ("chr2", 24) -> (15,  1)
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
      ("chr1", 10) -> ( 1,  1),
      ("chr1", 11) -> ( 6,  5),
      ("chr1", 12) -> (15, 10),
      ("chr1", 13) -> (16, 10),
      ("chr1", 14) -> (11,  5),
      ("chr1", 15) -> (12,  1)
    )(
      "chr1:10-12",
      "chr1:12-13",
      "chr1:14-16"
    )
  }

  test("skip extreme depth at end") {
    check(
      ("chr1", 10) -> ( 1,  1),
      ("chr1", 11) -> ( 6,  5),
      ("chr1", 12) -> (15, 10),
      ("chr1", 13) -> (16, 10)
    )(
      "chr1:10-12",
      "chr1:12-13"
    )
  }

  test("partition spanning several contigs") {
    check(
      ("chr1", 10) -> ( 1,  1),
      ("chr1", 11) -> ( 6,  5),
      ("chr2",  1) -> ( 2,  2),
      ("chr3",  7) -> ( 3,  3),
      ("chr3",  9) -> ( 4,  3),
      ("chr4", 20) -> ( 2,  2)
    )(
      "chr1:10-12,chr2:1-2,chr3:7-8,chr3:9-10",
      "chr4:20-21"
    )
  }
}
