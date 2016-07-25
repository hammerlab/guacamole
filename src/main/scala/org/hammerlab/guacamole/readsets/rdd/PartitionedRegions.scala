package org.hammerlab.guacamole.readsets.rdd

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.distributed.{KeyPartitioner, PileupFlatMapUtils, TaskPosition, WindowFlatMapUtils}
import org.hammerlab.guacamole.loci.partitioning.LociPartitioning
import org.hammerlab.guacamole.logging.DelayedMessages
import org.hammerlab.guacamole.logging.LoggingUtils.progress
import org.hammerlab.guacamole.readsets.PerSample
import org.hammerlab.guacamole.reference.ReferenceRegion
import org.hammerlab.guacamole.windowing.SlidingWindow

import scala.reflect.ClassTag

/**
 * This object encapsulates functionality for partitioning reference-mapped regions for processing as "pileups".
 *
 * Given an RDD of regions, it:
 *
 *   - partitions them by genomic loci,
 *   - makes multiple copies of regions that straddle partition boundaries.
 *
 * The result is that, given knowledge of which ranges of genomic loci correspond to which partition of the resulting
 * RDD, downstream code can iterate over the partitions emitted here and build full pileups for the loci that each
 * partition corresponds to.
 *
 * [[WindowFlatMapUtils]] calls this, and uses [[SlidingWindow]] to iterate over the partitioned regions in a manner
 * suitable for pileup-construction by [[PileupFlatMapUtils]].
 */
object PartitionedRegions {
  /**
   * @param regionRDDs RDDs of regions to partition.
   * @param lociPartitionsBoxed Spark broadcast of a [[LociPartitioning]] describing which ranges of genomic loci should
   *                            be mapped to each Spark partition.
   * @param halfWindowSize Send a copy of a region to a partition if it passes within this distance of that partition's
   *                       designated loci.
   * @tparam R ReferenceRegion type.
   * @return RDD of regions, keyed by the "sample ID" in @regionRDDs that they came from.
   */
  def apply[R <: ReferenceRegion: ClassTag](regionRDDs: PerSample[RDD[R]],
                                            lociPartitionsBoxed: Broadcast[LociPartitioning],
                                            halfWindowSize: Int): RDD[(Int, R)] = {

    val sc = regionRDDs(0).sparkContext

    val lociPartitions = lociPartitionsBoxed.value

    progress(s"Partitioning reads according to loci partitioning:\n$lociPartitions")

    // Counters
    val totalRegions = sc.accumulator(0L)
    val relevantRegions = sc.accumulator(0L)
    val expandedRegions = sc.accumulator(0L)

    DelayedMessages.default.say { () =>
      "Region counts: filtered %,d total regions to %,d relevant regions, expanded for overlaps by %,.2f%% to %,d".format(
        totalRegions.value,
        relevantRegions.value,
        (expandedRegions.value - relevantRegions.value) * 100.0 / relevantRegions.value,
        expandedRegions.value)
    }

    // Expand regions into (TaskPosition, region) pairs for each region RDD; the TaskPosition keys' `partition` field
    // will guide each pair to the designated partition in the `repartitionAndSortWithinPartitions` step below.
    val keyedRegionRDDs: PerSample[RDD[(TaskPosition, R)]] =
      regionRDDs.map(
        _.flatMap(region => {
          val singleContig = lociPartitionsBoxed.value.onContig(region.contigName)
          val partitionsForRegion = singleContig.getAll(region.start - halfWindowSize, region.end + halfWindowSize)

          // Update counters
          totalRegions += 1
          if (partitionsForRegion.nonEmpty) relevantRegions += 1
          expandedRegions += partitionsForRegion.size

          // Return this region, duplicated for each partition it is assigned to.
          partitionsForRegion.map(partition => TaskPosition(partition, region.contigName, region.start) -> region)
        })
      )

    val numPartitions = lociPartitions.inverse.map(_._1).max + 1

    // Build RDDs of (read-set index, read) and merge these into one RDD,
    sc
      .union(
        for {
          (keyedRegionsRDD, sampleId) <- keyedRegionRDDs.zipWithIndex
        } yield {
          for {
            (taskPosition, read) <- keyedRegionsRDD
          } yield
            taskPosition -> (sampleId, read)
        }
      )
      .repartitionAndSortWithinPartitions(KeyPartitioner(numPartitions))  // partition by TaskPosition.partition
      .values  // Drop TaskPosition keys, leave behind just (read-set index, region) pairs.
  }
}
