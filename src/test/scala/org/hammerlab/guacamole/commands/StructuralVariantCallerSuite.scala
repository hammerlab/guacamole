package org.hammerlab.guacamole.commands

import org.hammerlab.guacamole.util.{ TestUtil, GuacFunSuite }
import org.scalatest.Matchers

import scalax.collection.Graph
import scalax.collection.GraphPredef._
import scalax.collection.edge.Implicits._

class StructuralVariantCallerSuite extends GuacFunSuite with Matchers {

  // aliases
  val MedianStats = StructuralVariant.MedianStats
  val medianStats = StructuralVariant.Caller.medianStats[Int] _

  test("median stats") {
    // This example comes from Wikipedia: https://en.wikipedia.org/wiki/Median_absolute_deviation
    val nums = Array(2, 4, 1, 1, 2, 6, 9)
    assert(medianStats(nums) == MedianStats(2, 1))

    // An array with an even number of elements an a non-integer median/MAD
    val nums2 = Array(0, 1, 2, 2)
    assert(medianStats(nums2) == MedianStats(1.5, 0.5))

    // Pathological cases
    assert(medianStats(Array(1)) == MedianStats(1.0, 0.0))
    assert(medianStats(Array()) == MedianStats(0.0, 0.0))
  }

  sparkTest("read filtering") {
    val reads = Seq(
      // A few reads with an insert size of 100 to set the median and MAD
      // TODO: add reads which are not firstInPair
      TestUtil.makePairedMappedRead(start = 9, mateStart = 97),
      TestUtil.makePairedMappedRead(start = 10, mateStart = 98),
      TestUtil.makePairedMappedRead(start = 11, mateStart = 99),
      TestUtil.makePairedMappedRead(start = 12, mateStart = 100),
      TestUtil.makePairedMappedRead(start = 13, mateStart = 101),

      // An inverted read pair
      TestUtil.makePairedMappedRead(start = 100, mateStart = 150, isPositiveStrand = true, isMatePositiveStrand = true),

      // Two reads with an unusually large insert (300bp)
      TestUtil.makePairedMappedRead(start = 1000, mateStart = 1288),
      TestUtil.makePairedMappedRead(start = 1001, mateStart = 1289),

      // A read with an insert so large it should be filtered.
      TestUtil.makePairedMappedRead(start = 2000, mateStart = 2000000)
    )

    val result = StructuralVariant.Caller.getExceptionalReads(sc.parallelize(reads))
    // It should drop the inverted read pair & the pair with a very large insert
    assert(result.readsInRange.count == 7)
    assert(result.insertSizes.collect() === Seq(100, 100, 100, 100, 100, 300, 300))
    assert(result.insertStats == MedianStats(100, 0))
    assert(result.maxNormalInsertSize == 100)
    assert(result.exceptionalReads.collect().map(_.read.start) === Seq(1000, 1001))
  }

  test("graph construction") {
    // Reads 2 & 3 are compatible with each other, but not with Read 1
    val reads = Array(
      TestUtil.makePairedMappedRead(start = 100, mateStart = 288),
      TestUtil.makePairedMappedRead(start = 1000, mateStart = 1288),
      TestUtil.makePairedMappedRead(start = 1001, mateStart = 1289)
    )

    val graph = StructuralVariant.Caller.buildVariantGraph(reads, 100)
    assert(graph === Graph(reads(1) ~ reads(2) % 1))

    // TODO: test overlapping but incompatible pairs
    // TODO: test reads with mateStart < start
  }

}
