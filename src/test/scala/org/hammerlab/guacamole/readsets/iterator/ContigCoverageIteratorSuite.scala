package org.hammerlab.guacamole.readsets.iterator

import org.hammerlab.guacamole.loci.Coverage
import org.hammerlab.guacamole.loci.set.LociParser
import org.hammerlab.guacamole.readsets.rdd.TestRegion
import org.hammerlab.guacamole.readsets.{ContigLengths, ContigLengthsUtil}
import org.hammerlab.guacamole.reference.{ContigIterator, Position}
import org.scalatest.{FunSuite, Matchers}

class ContigCoverageIteratorSuite extends FunSuite with Matchers with ContigLengthsUtil {

  def check(contig: String,
            halfWindowSize: Int,
            intervals: (Int, Int)*)(
    expectedStrs: ((String, Int), (Int, Int, Int))*
  ): Unit =
    check("all", contig, halfWindowSize, intervals: _*)(expectedStrs: _*)

  def check(lociStr: String,
            contig: String,
            halfWindowSize: Int,
            intervals: (Int, Int)*)(
    expectedStrs: ((String, Int), (Int, Int, Int))*
  ): Unit = {

    val reads =
      (for {
        (start, end) <- intervals
      } yield
        TestRegion(contig, start, end)
      ).iterator.buffered

    val contigLengths: ContigLengths = makeContigLengths("chr1" -> 100, "chr2" -> 200)
    val loci = LociParser(lociStr).result(contigLengths).onContig(contig).iterator

    val expected =
      for {
        ((contig, locus), (depth, starts, ends)) <- expectedStrs
      } yield
        Position(contig, locus) -> Coverage(depth, starts, ends)

    ContigCoverageIterator(halfWindowSize, ContigIterator(reads), loci).toList should be(expected)
  }

  test("simple") {
    check(
      "chr1",
      halfWindowSize = 0,
      (10, 20)
    )(
      ("chr1", 10) -> (1, 1, 0),
      ("chr1", 11) -> (1, 0, 0),
      ("chr1", 12) -> (1, 0, 0),
      ("chr1", 13) -> (1, 0, 0),
      ("chr1", 14) -> (1, 0, 0),
      ("chr1", 15) -> (1, 0, 0),
      ("chr1", 16) -> (1, 0, 0),
      ("chr1", 17) -> (1, 0, 0),
      ("chr1", 18) -> (1, 0, 0),
      ("chr1", 19) -> (1, 0, 0),
      ("chr1", 20) -> (0, 0, 1)
    )
  }
}
