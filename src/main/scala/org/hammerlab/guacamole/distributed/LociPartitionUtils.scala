package org.hammerlab.guacamole.distributed

import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.Common.Arguments.{Base, Loci}
import org.hammerlab.guacamole.Common._
import org.hammerlab.guacamole.HasReferenceRegion
import org.hammerlab.guacamole.loci.{LociMap, LociSet}
import org.kohsuke.args4j.{Option => Args4jOption}

import scala.reflect.ClassTag

object LociPartitionUtils {

  trait Arguments extends Base with Loci {
    @Args4jOption(name = "--parallelism", usage = "Num variant calling tasks. Set to 0 (default) to use the number of Spark partitions.")
    var parallelism: Int = 0

    @Args4jOption(name = "--partition-accuracy",
      usage = "Num micro partitions to use per task in loci partitioning. Set to 0 to partition loci uniformly. Default: 250.")
    var partitioningAccuracy: Int = 250
  }

  /**
    * Partition a LociSet among tasks according to the strategy specified in args.
    */
  def partitionLociAccordingToArgs[M <: HasReferenceRegion: ClassTag](args: Arguments,
                                                                      loci: LociSet,
                                                                      regionRDDs: RDD[M]*): LociMap[Long] = {
    val sc = regionRDDs.head.sparkContext
    val tasks = if (args.parallelism > 0) args.parallelism else sc.defaultParallelism
    if (args.partitioningAccuracy == 0) {
      partitionLociUniformly(tasks, loci)
    } else {
      partitionLociByApproximateDepth(
        tasks,
        loci,
        args.partitioningAccuracy,
        regionRDDs: _*
      )
    }
  }

  /**
    * Assign loci from a LociSet to partitions. Contiguous intervals of loci will tend to get assigned to the same
    * partition.
    *
    * This implementation assigns loci uniformly, i.e. each task gets about the same number of loci. A smarter
    * implementation would know about the regions (depth of coverage), and try to assign each task loci corresponding to
    * about the same number of regions.
    *
    * @param tasks number of partitions
    * @param loci loci to partition
    * @return LociMap of locus -> task assignments
    */
  def partitionLociUniformly(tasks: Long, loci: LociSet): LociMap[Long] = {
    assume(tasks >= 1, "`tasks` (--parallelism) should be >= 1")
    val lociPerTask = math.max(1, loci.count.toDouble / tasks.toDouble)
    progress("Splitting loci evenly among %,d tasks = ~%,.0f loci per task".format(tasks, lociPerTask))
    val builder = LociMap.newBuilder[Long]
    var lociAssigned = 0L
    var task = 0L
    def remainingForThisTask = math.round(((task + 1) * lociPerTask) - lociAssigned)
    loci.contigs.foreach(contig => {
      loci.onContig(contig).ranges.foreach(range => {
        var start = range.start
        val end = range.end
        while (start < end) {
          val length: Long = math.min(remainingForThisTask, end - start)
          builder.put(contig, start, start + length, task)
          start += length
          lociAssigned += length
          if (remainingForThisTask == 0) task += 1
        }
      })
    })
    val result = builder.result
    assert(lociAssigned == loci.count)
    assert(result.count == loci.count)
    result
  }

  /**
    * Assign loci from a LociSet to partitions, where each partition overlaps about the same number of regions.
    *
    * The approach we take is:
    *
    *  (1) chop up the loci uniformly into many genomic "micro partitions."
    *
    *  (2) for each micro partition, calculate the number of regions that overlap it.
    *
    *  (3) using these counts, assign loci to real ("macro") partitions, making the approximation of uniform depth within
    *      each micro partition.
    *
    *  Some advantages of this approach are:
    *
    *  - Stages (1) and (3), which are done locally by the Spark master, are constant time with respect to the number
    *    of regions.
    *
    *  - Stage (2), where runtime does depend on the number of regions, is done in parallel with Spark.
    *
    *  - We can tune the accuracy vs. performance trade off by setting `microTasks`.
    *
    *  - Does not require a distributed sort.
    *
    * @param tasks number of partitions
    * @param lociUsed loci to partition
    * @param accuracy integer >= 1. Higher values of this will result in a more exact but also more expensive computation.
    *                 Specifically, this is the number of micro partitions to use per task to estimate the region depth.
    *                 In the extreme case, setting this to greater than the number of loci per task will result in an
    *                 exact calculation.
    * @param regionRDDs: region RDD 1, region RDD 2, ...
    *                Any number RDD[ReferenceRegion] arguments giving the regions to base the partitioning on.
    * @return LociMap of locus -> task assignments.
    */
  def partitionLociByApproximateDepth[M <: HasReferenceRegion: ClassTag](tasks: Int,
                                                                         lociUsed: LociSet,
                                                                         accuracy: Int,
                                                                         regionRDDs: RDD[M]*): LociMap[Long] = {
    val sc = regionRDDs(0).sparkContext

    // Step (1). Split loci uniformly into micro partitions.
    assume(tasks >= 1)
    assume(lociUsed.count > 0)
    assume(regionRDDs.nonEmpty)
    val numMicroPartitions: Int = if (accuracy * tasks < lociUsed.count) accuracy * tasks else lociUsed.count.toInt
    progress("Splitting loci by region depth among %,d tasks using %,d micro partitions.".format(tasks, numMicroPartitions))
    val microPartitions = partitionLociUniformly(numMicroPartitions, lociUsed)
    progress("Done calculating micro partitions.")
    val broadcastMicroPartitions = sc.broadcast(microPartitions)

    // Step (2)
    // Total up regions overlapping each micro partition. We keep the totals as an array of Longs.
    var num = 1
    val regionCounts = regionRDDs.map(regions => {
      progress("Collecting region counts for RDD %d of %d.".format(num, regionRDDs.length))
      val result = regions.flatMap(region =>
        broadcastMicroPartitions.value.onContig(region.referenceContig).getAll(region.start, region.end)
      ).countByValue()
      progress("RDD %d: %,d regions".format(num, result.values.sum))
      num += 1
      result
    })

    val counts: Seq[Long] = (0 until numMicroPartitions).map(i => regionCounts.map(_.getOrElse(i, 0L)).sum)

    // Step (3)
    // Assign loci to tasks, taking into account region depth in each micro partition.
    val totalRegions = counts.sum
    val regionsPerTask = math.max(1, totalRegions.toDouble / tasks.toDouble)
    progress("Done collecting region counts. Total regions with micro partition overlaps: %,d = ~%,.0f regions per task."
             .format(totalRegions, regionsPerTask))

    val maxIndex = counts.view.zipWithIndex.maxBy(_._1)._2
    progress("Regions per micro partition: min=%,d mean=%,.0f max=%,d at %s.".format(
      counts.min, counts.sum.toDouble / counts.length, counts(maxIndex), microPartitions.asInverseMap(maxIndex)))

    val builder = LociMap.newBuilder[Long]
    var regionsAssigned = 0.0
    var task = 0L
    def regionsRemainingForThisTask = math.round(((task + 1) * regionsPerTask) - regionsAssigned)
    var microTask = 0
    while (microTask < numMicroPartitions) {
      var set = microPartitions.asInverseMap(microTask)
      var regionsInSet = counts(microTask)
      while (!set.isEmpty) {
        if (regionsInSet == 0) {
          // Take the whole set if there are no regions assigned to it.
          builder.put(set, task)
          set = LociSet.empty
        } else {
          // If we've allocated all regions for this task, move on to the next task.
          if (regionsRemainingForThisTask == 0)
            task += 1
          assert(regionsRemainingForThisTask > 0)
          assert(task < tasks)

          // Making the approximation of uniform depth within each micro partition, we assign a proportional number of
          // loci and regions to the current task. The proportion of loci we assign is the ratio of how many regions we have
          // remaining to allocate for the current task vs. how many regions are remaining in the current micro partition.

          // Here we calculate the fraction of the current micro partition we are going to assign to the current task.
          // May be 1.0, in which case all loci (and therefore regions) for this micro partition will be assigned to the
          // current task.
          val fractionToTake = math.min(1.0, regionsRemainingForThisTask.toDouble / regionsInSet.toDouble)

          // Based on fractionToTake, we set the number of loci and regions to assign.
          // We always take at least 1 locus to ensure we continue to make progress.
          val lociToTake = math.max(1, (fractionToTake * set.count).toLong)
          val regionsToTake = (fractionToTake * regionsInSet).toLong

          // Add the new task assignment to the builder, and update bookkeeping info.
          val (currentSet, remainingSet) = set.take(lociToTake)
          builder.put(currentSet, task)
          regionsAssigned += math.round(regionsToTake).toLong
          regionsInSet -= math.round(regionsToTake).toLong
          set = remainingSet
        }
      }
      microTask += 1
    }
    val result = builder.result
    assert(result.count == lociUsed.count)
    result
  }
}
