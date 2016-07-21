package org.hammerlab.guacamole.loci.partitioning

import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.reads.MappedRead
import org.hammerlab.guacamole.util.{GuacFunSuite, TestUtil}

class MicroRegionPartitionerSuite extends GuacFunSuite {
  test("partitionLociByApproximateReadDepth") {
    def makeRead(start: Long, length: Long) = {
      TestUtil.makeRead("A" * length.toInt, "%sM".format(length), start, "chr1")
    }
    def pairsToReads(pairs: Seq[(Long, Long)]): RDD[MappedRead] = {
      sc.parallelize(pairs.map(pair => makeRead(pair._1, pair._2)))
    }

    val reads = pairsToReads(Seq(
      (5L, 1L),
      (6L, 1L),
      (7L, 1L),
      (8L, 1L)))
    val loci = LociSet("chr1:0-100")

    val result =
      new MicroRegionPartitioner(
        reads,
        halfWindowSize = 0,
        numPartitions = 2,
        microPartitionsPerPartition = 100
      ).partition(loci)

    result.toString should equal("chr1:0-7=0,chr1:7-100=1")
  }
}
