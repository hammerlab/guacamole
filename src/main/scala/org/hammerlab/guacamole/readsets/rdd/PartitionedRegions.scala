package org.hammerlab.guacamole.readsets.rdd

import org.apache.spark.Accumulable
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.distributed.{PileupFlatMapUtils, WindowFlatMapUtils}
import org.hammerlab.guacamole.loci.partitioning.LociPartitioner.PartitionIndex
import org.hammerlab.guacamole.loci.partitioning.LociPartitioning
import org.hammerlab.guacamole.logging.LoggingUtils.progress
import org.hammerlab.guacamole.readsets.{PerSample, SampleId}
import org.hammerlab.guacamole.reference.ReferenceRegion
import org.hammerlab.guacamole.windowing.SlidingWindow
import org.hammerlab.magic.accumulables.{HistogramParam, HashMap => MagicHashMap}
import org.hammerlab.magic.rdd.KeyPartitioner
import org.hammerlab.magic.stats.Stats

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

  type IntHist = MagicHashMap[Int, Long]

  def IntHist(): IntHist = MagicHashMap[Int, Long]()

  /**
   * @param regionRDDs RDDs of regions to partition.
   * @param partitioningBroadcast Spark broadcast of a [[LociPartitioning]] describing which ranges of genomic loci
   *                              should be mapped to each Spark partition.
   * @param halfWindowSize Send a copy of a region to a partition if it passes within this distance of that partition's
   *                       designated loci.
   * @tparam R ReferenceRegion type.
   * @return RDD of regions, keyed by the "sample ID" in @regionRDDs that they came from.
   */
  def apply[R <: ReferenceRegion: ClassTag](regionRDDs: PerSample[RDD[R]],
                                            partitioningBroadcast: Broadcast[LociPartitioning],
                                            halfWindowSize: Int,
                                            printStats: Boolean = true): RDD[(SampleId, R)] = {

    val sc = regionRDDs(0).sparkContext

    val lociPartitioning = partitioningBroadcast.value

    val numPartitions = lociPartitioning.numPartitions

    progress(s"Partitioning reads according to loci partitioning:\n$lociPartitioning")

    implicit val accumulableParam = new HistogramParam[Int, Long]

    // Combine the RDDs, with each region keyed by its sample ID.
    val keyedRegions: RDD[(SampleId, R)] =
      sc
      .union(
        for {
          (regionsRDD, sampleId) <- regionRDDs.zipWithIndex
        } yield {
          for {
            region <- regionsRDD
          } yield
            sampleId -> region
        }
      )

    // Histogram of the number of copies made of each region (i.e. when a region straddles loci-partition
    // boundaries.
    val regionCopiesHistogram: Accumulable[IntHist, Int] = sc.accumulable(IntHist(), "copies-per-region")

    // Histogram of the number of regions assigned to each partition.
    val partitionRegionsHistogram: Accumulable[IntHist, Int] = sc.accumulable(IntHist(), "regions-per-partition")

    // Emit a copy of each region for each partition's alloted loci that it overlaps.
    val partitionedRegions: RDD[(SampleId, R)] =
      (for {
        // For each region…
        (sampleId, region) <- keyedRegions

        // Partitions that should receive a copy of this region.
        partitions = partitioningBroadcast.value.getAll(region, halfWindowSize)

        // Update the copies-per-region histogram accumulator.
        _ = (regionCopiesHistogram += partitions.size)

        // For each destination partition…
        partition <- partitions
      } yield {

        // Update the regions-per-partition histogram accumulator.
        partitionRegionsHistogram += partition

        // Key (sample-id, region) pairs by a tuple that will direct it to the correct partition, with secondary fields
        // that will be used by intra-partition sorting.
        (partition, region.contigName, region.start) -> (sampleId, region)
      })
      .repartitionAndSortWithinPartitions(KeyPartitioner(numPartitions))  // Shuffle all region copies.
      .values  // Drop the destination partition / sorting tuple-key; leave only the (sample id, region) pairs.
      .setName("partitioned-regions")

    if (printStats) {
      // Need to force materialization for the accumulator to have data… but that's reasonable because anything
      // downstream is presumably going to reuse this RDD.
      val totalReadCopies = partitionedRegions.count

      val originalReads = keyedRegions.count

      // Sorted array of [number of read copies "K"] -> [number of reads that were copied "K" times].
      val regionCopies: Array[(Int, Long)] = regionCopiesHistogram.value.toArray.sortBy(_._1)

      // Number of distinct reads that were sent to at least one partition.
      val readsPlaced = regionCopies.filter(_._1 > 0).map(_._2).sum

      // Sorted array: [partition index "K"] -> [number of reads assigned to partition "K"].
      val regionsPerPartition: Array[(PartitionIndex, Long)] = partitionRegionsHistogram.value.toArray.sortBy(_._1)

      progress(
        s"Placed $readsPlaced of $originalReads (%.1f%%), %.1fx copies on avg; copies per read histogram:"
        .format(
          100.0 * readsPlaced / originalReads,
          totalReadCopies * 1.0 / readsPlaced
        ),
        Stats.fromHist(regionCopies).toString(),
        "",
        "Reads per partition stats:",
        Stats(regionsPerPartition.map(_._2)).toString()
      )
    }

    partitionedRegions
  }
}
