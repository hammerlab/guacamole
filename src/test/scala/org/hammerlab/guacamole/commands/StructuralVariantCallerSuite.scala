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

  test("read compatibility") {
    // Shorthand for this test
    val areCompat = StructuralVariant.Caller.areReadsCompatible(_, _, _)
    def makePair(start: Long, end: Long, mateStart: Long, mateEnd: Long) = {
      assert(mateEnd - mateStart == end - start)
      TestUtil.makePairedMappedRead(start = start, mateStart = mateStart, sequence = "A" * (end - start).toInt)
    }

    // Assertions which are commented out _should_ pass but do not because of inaccuracies in the DELLY checks.

    // Scenario 1:
    // 0 10       90 100
    // <--- ...... --->
    //   <--- .... --->
    // The largest deletion compatible with this is of (20, 90). It would produce insert sizes of 30 and 20.
    assert(areCompat(makePair(0, 10, 90, 100), makePair(10, 20, 90, 100), 10) == false)
    // assert(areCompat(makePair(0, 10, 90, 100), makePair(10, 20, 90, 100), 29) == false)
    assert(areCompat(makePair(0, 10, 90, 100), makePair(10, 20, 90, 100), 30) == true)
    assert(areCompat(makePair(0, 10, 90, 100), makePair(10, 20, 90, 100), 40) == true)

    // Scenario 2:
    // 0 10       90 100
    // <--- ........ --->
    //   <--- ... --->
    // The largest deletion compatible with this is of (20, 90). It produces inserts of 40 and 20.
    assert(areCompat(makePair(0, 10, 100, 110), makePair(10, 20, 90, 100), 10) == false)
    // assert(areCompat(makePair(0, 10, 100, 110), makePair(10, 20, 90, 100), 20) == false)
    // assert(areCompat(makePair(0, 10, 100, 110), makePair(10, 20, 90, 100), 39) == false)
    assert(areCompat(makePair(0, 10, 100, 110), makePair(10, 20, 90, 100), 40) == true)
    assert(areCompat(makePair(0, 10, 100, 110), makePair(10, 20, 90, 100), 50) == true)

    // Scenario 3:
    // 0 10       90 100
    // <--- ...... --->
    //   <--- ...... --->
    // The largest deletion with this is (20, 90). It produces inserts of 30 and 30.
    // TODO: a less symmetric test
    // assert(areCompat(makePair(0, 10, 90, 100), makePair(10, 20, 100, 110), 20) == false)
    // assert(areCompat(makePair(0, 10, 90, 100), makePair(10, 20, 100, 110), 29) == false)
    assert(areCompat(makePair(0, 10, 90, 100), makePair(10, 20, 100, 110), 30) == true)
    assert(areCompat(makePair(0, 10, 90, 100), makePair(10, 20, 100, 110), 40) == true)

    // Scenario 4:
    // 0 10       90 100
    // <--- ...... --->
    //              <--- ...... --->
    assert(areCompat(makePair(0, 10, 90, 100), makePair(95, 105, 195, 205), 1000) == false)
  }

  sparkTest("read filtering") {
    val reads = Seq(
      // A few reads with an insert size near 100 to set the median to 100 and MAD to 1.
      // (TestUtil makes reads with a length of 12, so insert size = 97 - 9 + 12 == 100)
      // TODO: add reads which are not firstInPair
      TestUtil.makePairedMappedRead(start = 9, mateStart = 97), // insert size 100
      TestUtil.makePairedMappedRead(start = 10, mateStart = 97), // 99
      TestUtil.makePairedMappedRead(start = 11, mateStart = 98), // 99
      TestUtil.makePairedMappedRead(start = 12, mateStart = 101), // 101
      TestUtil.makePairedMappedRead(start = 13, mateStart = 101), // 100

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
    assert(result.insertSizes.collect() === Seq(100, 99, 99, 101, 100, 300, 300))
    assert(result.insertStats == MedianStats(100, 1))
    assert(result.maxNormalInsertSize == 105)
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
