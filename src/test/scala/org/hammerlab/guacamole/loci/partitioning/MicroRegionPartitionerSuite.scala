package org.hammerlab.guacamole.loci.partitioning

import org.apache.spark.rdd.RDD
import org.hammerlab.genomics.loci.set.LociSet
import org.hammerlab.genomics.loci.set.test.TestLociSet
import org.hammerlab.guacamole.reads.{MappedRead, ReadsUtil}
import org.hammerlab.guacamole.util.GuacFunSuite

class MicroRegionPartitionerSuite
  extends GuacFunSuite
    with ReadsUtil {

  test("partition") {

    def pairsToReads(pairs: Seq[(Long, Long)]): RDD[MappedRead] =
      sc.parallelize(
        for {
          (start, length) <- pairs
        } yield
          makeRead(
            sequence = "A" * length.toInt,
            cigar = "%sM".format(length),
            start = start
          )
      )

    val reads =
      pairsToReads(
        Seq(
          (5L, 1L),
          (6L, 1L),
          (7L, 1L),
          (8L, 1L)
        )
      )

    val loci = TestLociSet("chr1:0-100")

    val result =
      new MicroRegionPartitioner(
        reads,
        numPartitions = 2,
        microPartitionsPerPartition = 100
      ).partition(loci)

    result.toString should equal("chr1:0-7=0,chr1:7-100=1")
  }
}
