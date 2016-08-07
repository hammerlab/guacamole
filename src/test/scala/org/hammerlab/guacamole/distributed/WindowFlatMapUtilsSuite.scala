package org.hammerlab.guacamole.distributed

import org.hammerlab.guacamole.loci.partitioning.UniformPartitioner
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.reads.MappedRead
import org.hammerlab.guacamole.util.{GuacFunSuite, TestUtil}
import org.hammerlab.guacamole.windowing.SlidingWindow
import WindowFlatMapUtils.windowFoldLoci
import org.hammerlab.guacamole.readsets.rdd.ReadsRDDUtil

class WindowFlatMapUtilsSuite
  extends GuacFunSuite
  with ReadsRDDUtil {

  test("test window fold parallelism 5; average read depth") {

    // 4 overlapping reads starting at loci = 0
    //     0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6
    // r1: T C G A T C G G
    // r2:   C C C C C C C C
    // r3:         T C G A T C G A
    // r4:                   G G G G G G G
    // At pos = 0, the depth is 1
    // At pos = 1, 2, 3,  the depth is 2
    // At pos = 4 through 7, the depth is 3
    // At pos = 8 - 11 through 11, the depth is 2

    val reads =
      makeReadsRDD(
        ("TCGATCGGC", "8M", 0),
        ("CCCCCCCC", "8M", 1),
        ("TCGATCGA", "8M", 4),
        ("GGGGGGG", "7M", 9)
      )

    val counts =
      windowFoldLoci(
        Vector(reads),
        // Split loci in 5 partitions - we will compute an aggregate value per partition
        UniformPartitioner(5).partition(LociSet("chr1:0-20")),
        skipEmpty = false,
        halfWindowSize = 0,
        initialValue = (0L, 0L),
        // averageDepth is represented as fraction tuple (numerator, denominator) == (totalDepth, totalLoci)
        (averageDepth: (Long, Long), windows: Seq[SlidingWindow[MappedRead]]) => {
          val currentDepth = windows.map(w => w.currentRegions().count(_.overlapsLocus(w.currentLocus))).sum
          (averageDepth._1 + currentDepth, averageDepth._2 + 1)
        }
      ).collect()

    counts.length should be(5)
    counts(0) should be(7, 4)   // average depth between [0, 3] is 7/4 = 2.75
    counts(1) should be(12, 4)  // average depth between [4, 7] is 12/4 = 3
    counts(2) should be(8, 4)   // average depth between [8, 11] is 8/4 = 2
    counts(3) should be(4, 4)   // average depth between [12, 15] is 4/4 = 2
    counts(4) should be(0, 4)   // average depth between [16, 19] is 0/4 = 0
  }
}
