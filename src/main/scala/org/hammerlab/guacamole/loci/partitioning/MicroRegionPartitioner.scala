package org.hammerlab.guacamole.loci.partitioning

import org.apache.spark.rdd.RDD
import org.hammerlab.genomics.loci.map.LociMap
import org.hammerlab.genomics.loci.set.LociSet
import org.hammerlab.genomics.reference.Region
import org.hammerlab.guacamole.loci.partitioning.MicroRegionPartitioner.{MicroPartitionIndex, NumMicroPartitions}
import org.hammerlab.guacamole.logging.LoggingUtils.progress
import org.hammerlab.spark.{NumPartitions, PartitionIndex}
import org.kohsuke.args4j.{Option => Args4jOption}

import scala.collection.Map
import scala.reflect.ClassTag

trait MicroRegionPartitionerArgs extends UniformPartitionerArgs {

  /**
   * Long >= 1. Number of micro-partitions generated for each of the `numPartitions` Spark partitions that will be
   * computed. Higher values of this will result in a more exact but more expensive computation.
   * In the extreme, setting this to greater than the number of loci (per partition) will result in an exact
   * calculation.
   */
  @Args4jOption(
    name = "--partition-accuracy",
    usage = "Number of micro-partitions to use, per Spark partition, when partitioning loci (default: 250). Set to 0 to partition loci uniformly"
  )
  var partitioningAccuracy: NumMicroPartitions = 250
}

/**
 * Assign loci from a LociSet to partitions, where each partition overlaps approximately the same number of "regions"
 * (reads mapped to a reference genome).
 *
 * The approach we take is:
 *
 *  (1) chop up the loci uniformly into many genomic "micro partitions."
 *
 *  (2) for each micro partition, calculate the number of regions that overlap it.
 *
 *  (3) using these counts, assign loci to real (Spark) partitions, assuming approximately uniform depth within each
 *      micro partition.
 *
 *  Some advantages of this approach are:
 *
 *  - Stages (1) and (3), which are done locally by the Spark master, are constant time with respect to the number
 *    of regions (though linear in the number of micro-partitions).
 *
 *  - Stage (2), where runtime does depend on the number of regions, is done in parallel with Spark.
 *
 *  - Accuracy vs. performance can be tuned by setting `accuracy`.
 *
 *  - Does not require a distributed sort.
 *
 * @param regions: RDD of reads to base the partitioning on.
 * @param numPartitions Number of partitions to split reads into.
 * @param microPartitionsPerPartition Long >= 1. Number of micro-partitions generated for each of the `numPartitions`
 *                                    Spark partitions that will be computed. Higher values of this will result in a
 *                                    more exact but more expensive computation.
 *                                    In the extreme, setting this to greater than the number of loci (per partition)
 *                                    will result in an exact calculation.
 * @return LociMap of locus -> partition assignments.
 */
class MicroRegionPartitioner[R <: Region: ClassTag](regions: RDD[R],
                                                    numPartitions: NumPartitions,
                                                    microPartitionsPerPartition: NumMicroPartitions)
  extends LociPartitioner {

  def partition(loci: LociSet): LociPartitioning = {

    assume(numPartitions >= 1)
    assume(loci.count > 0)

    val sc = regions.sparkContext

    // Step 1: split loci uniformly into micro partitions.
    val numMicroPartitions: NumMicroPartitions =
      if (microPartitionsPerPartition * numPartitions < loci.count)
        microPartitionsPerPartition * numPartitions
      else
        loci.count

    progress(
      "Splitting loci by region depth among %,d Spark partitions using %,d micro partitions."
      .format(numPartitions, numMicroPartitions)
    )

    val lociToMicroPartitionMap = UniformMicroPartitioner(numMicroPartitions).partitionsMap(loci)
    val microPartitionToLociMap = lociToMicroPartitionMap.inverse

    progress("Done calculating micro partitions.")

    val broadcastMicroPartitions = sc.broadcast(lociToMicroPartitionMap)

    // Step 2: total up regions overlapping each micro partition. We keep the totals as an array of Longs.
    val regionsPerMicroPartition: Map[MicroPartitionIndex, Long] =
      regions
        .flatMap(region =>
          broadcastMicroPartitions
            .value
            .onContig(region.contigName)
            .getAll(region.start, region.end)
        )
        .countByValue()

    // Step 3: assign loci to partitions, taking into account region depth in each micro partition.
    val totalRegions = regionsPerMicroPartition.values.sum
    val regionsPerPartition = math.max(1, totalRegions / numPartitions.toDouble)

    progress(
      "Done collecting region counts. Total regions with micro partition overlaps: %,d = ~%,.0f regions per partition."
      .format(totalRegions, regionsPerPartition)
    )

    val maxIndex = regionsPerMicroPartition.maxBy(_._2)._1.toInt

    val counts: Seq[Long] = (0L until numMicroPartitions).map(i => regionsPerMicroPartition.getOrElse(i, 0L))

    progress(
      "Regions per micro partition: min=%,d mean=%,.0f max=%,d at %s.".format(
        counts.min,
        counts.sum.toDouble / counts.length,
        counts(maxIndex),
        microPartitionToLociMap(maxIndex)
      )
    )

    var totalRegionsAssigned = 0.0
    var partition = 0
    def regionsRemainingForThisPartition =
      math.round(
        ((partition + 1) * regionsPerPartition) - totalRegionsAssigned
      )

    val builder = LociMap.newBuilder[PartitionIndex]

    var microPartition = 0
    while (microPartition < numMicroPartitions) {
      var set = microPartitionToLociMap(microPartition)
      var regionsInSet = counts(microPartition)
      while (!set.isEmpty) {
        if (regionsInSet == 0) {
          // Take the whole set if there are no regions assigned to it.
          builder.put(set, partition)
          set = LociSet()
        } else {

          // If we've allocated all regions for this partition, move on to the next partition.
          if (regionsRemainingForThisPartition == 0)
            partition += 1

          assert(regionsRemainingForThisPartition > 0)
          assert(partition < numPartitions)

          /**
           * Making the approximation of uniform depth within each micro partition, we assign a proportional number of
           * loci and regions to the current partition. The proportion of loci we assign is the ratio of how many
           * regions we have remaining to allocate for the current partition vs. how many regions are remaining in the
           * current micro partition.
           *
           * Here we calculate the fraction of the current micro partition we are going to assign to the current
           * partition.
           *
           * May be 1.0, in which case all loci (and therefore regions) for this micro partition will be assigned to the
           * current partition.
           *
           */
          val fractionToTake = math.min(1.0, regionsRemainingForThisPartition.toDouble / regionsInSet.toDouble)

          /**
           * Based on fractionToTake, we set the number of loci and regions to assign.
           *
           * We always take at least 1 locus to ensure we continue to make progress.
           */
          val lociToTake = math.max(1, (fractionToTake * set.count).toLong)
          val regionsToTake = (fractionToTake * regionsInSet).toLong

          // Add the new partition assignment to the builder, and update bookkeeping info.
          val (currentSet, remainingSet) = set.take(lociToTake)
          builder.put(currentSet, partition)
          totalRegionsAssigned += math.round(regionsToTake).toLong
          regionsInSet -= math.round(regionsToTake).toLong
          set = remainingSet
        }
      }
      microPartition += 1
    }
    val result = builder.result()
    assert(result.count == loci.count, s"Expected ${loci.count} loci, got ${result.count}")
    result
  }
}

object MicroRegionPartitioner {
  /**
   * Convenience types representing "micro-partitions" indices, or numbers of micro-partitions.
   *
   * Micro-partitions can be as numerous as loci in the genome, so we use Longs to represent them.
   *
   * Many of them can be combined into individual Spark partitions (which are Ints).
   */
  type MicroPartitionIndex = Long
  type NumMicroPartitions = Long
}
