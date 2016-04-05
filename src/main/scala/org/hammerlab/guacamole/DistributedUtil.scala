/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hammerlab.guacamole

import org.apache.commons.math3
import org.apache.spark.rdd.RDD
import org.apache.spark.{ Partitioner, AccumulatorParam, SparkConf, Logging }
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.serializer.JavaSerializer
import org.hammerlab.guacamole.Common.Arguments.{ Base, Loci }
import org.hammerlab.guacamole.Common._
import org.hammerlab.guacamole.pileup.Pileup
import org.hammerlab.guacamole.reads.MappedRead
import org.hammerlab.guacamole.reference.{ ContigSequence, ReferenceGenome }
import org.hammerlab.guacamole.windowing.{ SplitIterator, SlidingWindow }
import org.kohsuke.args4j.{ Option => Args4jOption }

import scala.collection.mutable.{ HashMap => MutableHashMap }
import scala.reflect.ClassTag

/**
 * Primitives for analyzing mapped reads with Spark.
 */
object DistributedUtil extends Logging {
  trait Arguments extends Base with Loci {
    @Args4jOption(name = "--parallelism", usage = "Num variant calling tasks. Set to 0 (default) to use the number of Spark partitions.")
    var parallelism: Int = 0

    @Args4jOption(name = "--partition-accuracy",
      usage = "Num micro partitions to use per task in loci partitioning. Set to 0 to partition loci uniformly. Default: 250.")
    var partitioningAccuracy: Int = 250
  }

  type PerSample[A] = IndexedSeq[A]

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
        regionRDDs: _*)
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
   * Given a LociSet and an RDD of regions, returns the same LociSet but with any contigs that don't have any regions
   * mapped to them removed. Also prints out progress info on the number of regions assigned to each contig.
   */
  def filterLociWhoseContigsHaveNoRegions[M <: HasReferenceRegion](loci: LociSet, regions: RDD[M]): LociSet = {
    val contigsInLociSet = loci.contigs.toSet
    val contigsAndCounts = regions.map(_.referenceContig).filter(contigsInLociSet.contains(_)).countByValue.toMap.withDefaultValue(0L)
    Common.progress("Region counts per contig: %s".format(
      contigsAndCounts.toSeq.sorted.map(pair => "%s=%,d".format(pair._1, pair._2)).mkString(" ")))
    val contigsWithoutRegions = loci.contigs.filter(contigsAndCounts(_) == 0L).toSet
    if (contigsWithoutRegions.nonEmpty) {
      Common.progress("Filtering out these contigs, since they have no overlapping regions: %s".format(
        contigsWithoutRegions.toSeq.sorted.mkString(", ")))
      loci.filterContigs(!contigsWithoutRegions.contains(_))
    } else {
      loci
    }
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

  /**
   * Helper function. Given optionally an existing Pileup, and a sliding read window return a new Pileup at the given
   * locus. If an existing Pileup is given as input, then the result will share elements with that Pileup for efficiency.
   *
   *  If an existing Pileup is provided, then its locus must be <= the new locus.
   */
  private def initOrMovePileup(existing: Option[Pileup],
                               window: SlidingWindow[MappedRead],
                               referenceContigSequence: ContigSequence): Pileup = {
    assume(window.halfWindowSize == 0)
    existing match {
      case None => Pileup(
        window.currentRegions(), window.referenceName, window.currentLocus, referenceContigSequence)
      case Some(pileup) => pileup.atGreaterLocus(window.currentLocus, window.newRegions.iterator)
    }
  }

  /**
   * Flatmap across loci, where at each locus the provided function is passed a Pileup instance.
   *
   * @param skipEmpty If true, the function will only be called at loci that have nonempty pileups, i.e. those
   *                  where at least one read overlaps. If false, then the function will be called at all the
   *                  specified loci. In cases where whole contigs may have no reads mapped (e.g. if running on
   *                  only a single chromosome, but loading loci from a sequence dictionary that includes the
   *                  entire genome), this is an important optimization.
   * @see the windowTaskFlatMapMultipleRDDs function for other argument descriptions
   *
   */
  def pileupFlatMap[T: ClassTag](reads: RDD[MappedRead],
                                 lociPartitions: LociMap[Long],
                                 skipEmpty: Boolean,
                                 reference: ReferenceGenome,
                                 function: Pileup => Iterator[T]): RDD[T] =
    pileupFlatMapBroadcast(reads, lociPartitions, skipEmpty, reads.context.broadcast(reference), function)

  def pileupFlatMapBroadcast[T: ClassTag](
    reads: RDD[MappedRead],
    lociPartitions: LociMap[Long],
    skipEmpty: Boolean,
    referenceBroadcast: Broadcast[ReferenceGenome],
    function: Pileup => Iterator[T]): RDD[T] = {

    windowFlatMapWithState(
      Vector(reads),
      lociPartitions,
      skipEmpty,
      halfWindowSize = 0,
      initialState = None,
      function =
        (maybePileup: Option[Pileup], windows: PerSample[SlidingWindow[MappedRead]]) => {
          assert(windows.length == 1)
          val contigSequence = referenceBroadcast.value.getContig(windows(0).referenceName)
          val pileup = initOrMovePileup(
            maybePileup,
            windows(0),
            contigSequence
          )
          (Some(pileup), function(pileup))
        }
    )
  }
  /**
   * Flatmap across loci on two RDDs of MappedReads. At each locus the provided function is passed two Pileup instances,
   * giving the pileup for the reads in each RDD at that locus.
   *
   * @param skipEmpty see [[pileupFlatMap]] for description.
   * @see the windowTaskFlatMapMultipleRDDs function for other argument descriptions.
   *
   */
  def pileupFlatMapTwoRDDs[T: ClassTag](reads1: RDD[MappedRead],
                                        reads2: RDD[MappedRead],
                                        lociPartitions: LociMap[Long],
                                        skipEmpty: Boolean,
                                        reference: ReferenceGenome,
                                        function: (Pileup, Pileup) => Iterator[T]): RDD[T] =
    pileupFlatMapTwoRDDsBroadcast(
      reads1,
      reads2,
      lociPartitions,
      skipEmpty,
      reads1.context.broadcast(reference),
      function
    )

  def pileupFlatMapTwoRDDsBroadcast[T: ClassTag](
    reads1: RDD[MappedRead],
    reads2: RDD[MappedRead],
    lociPartitions: LociMap[Long],
    skipEmpty: Boolean,
    referenceBroadcast: Broadcast[ReferenceGenome],
    function: (Pileup, Pileup) => Iterator[T]): RDD[T] = {

    windowFlatMapWithState(
      Vector(reads1, reads2),
      lociPartitions,
      skipEmpty,
      halfWindowSize = 0L,
      initialState = None,
      function =
        (maybePileups: Option[(Pileup, Pileup)], windows: PerSample[SlidingWindow[MappedRead]]) => {
          assert(windows.length == 2)
          val contigSequence = referenceBroadcast.value.getContig(windows(0).referenceName)
          val pileup1 = initOrMovePileup(maybePileups.map(_._1), windows(0), contigSequence)
          val pileup2 = initOrMovePileup(maybePileups.map(_._2), windows(1), contigSequence)
          (Some((pileup1, pileup2)), function(pileup1, pileup2))
        }
    )
  }

  /**
   * Flatmap across loci and any number of RDDs of MappedReads.
   *
   * @see the windowTaskFlatMapMultipleRDDs function for other argument descriptions.
   */
  def pileupFlatMapMultipleRDDs[T: ClassTag](
    readsRDDs: PerSample[RDD[MappedRead]],
    lociPartitions: LociMap[Long],
    skipEmpty: Boolean,
    reference: ReferenceGenome,
    function: PerSample[Pileup] => Iterator[T]
  ): RDD[T] =
    pileupFlatMapMultipleRDDsBroadcast(
      readsRDDs,
      lociPartitions,
      skipEmpty,
      readsRDDs.head.context.broadcast(reference),
      function
    )

  def pileupFlatMapMultipleRDDsBroadcast[T: ClassTag](
    readsRDDs: PerSample[RDD[MappedRead]],
    lociPartitions: LociMap[Long],
    skipEmpty: Boolean,
    referenceBroadcast: Broadcast[ReferenceGenome],
    function: PerSample[Pileup] => Iterator[T]): RDD[T] = {

    windowFlatMapWithState(
      readsRDDs,
      lociPartitions,
      skipEmpty,
      halfWindowSize = 0L,
      initialState = None,
      function = (maybePileups: Option[PerSample[Pileup]], windows: PerSample[SlidingWindow[MappedRead]]) => {

        val referenceContig = referenceBroadcast.value.getContig(windows(0).referenceName)

        val advancedPileups = maybePileups match {
          case Some(existingPileups) =>
            existingPileups.zip(windows).map(
              pileupAndWindow => initOrMovePileup(
                Some(pileupAndWindow._1),
                pileupAndWindow._2,
                referenceContig
              )
            )
          case None => windows.map(
            initOrMovePileup(None, _, referenceContig)
          )
        }
        (Some(advancedPileups), function(advancedPileups))
      }
    )
  }

  /**
   * FlatMap across loci, and any number of RDDs of regions, where at each locus the provided function is passed a
   * sliding window instance for each RDD containing the regions overlapping an interval of halfWindowSize to either side
   * of a locus.
   *
   * This function supports maintaining some state from one locus to another within a task. The state maintained is of type
   * S. The user function will receive the current state in addition to the sliding windows, and returns a pair of
   * (new state, result data). The state is initialized to initialState for each task, and for each new contig handled
   * by a single task.
   *
   * @param regionRDDs RDDs of reads, one per sample
   * @param lociPartitions loci to consider, partitioned into tasks
   * @param skipEmpty If True, then the function will only be called on loci where at least one region maps within a
   *                  window around the locus. If False, then the function will be called at all loci in lociPartitions.
   * @param halfWindowSize if another region overlaps a halfWindowSize to either side of a locus under consideration,
   *                       then it is included.
   * @param initialState initial state to use for each task and each contig analyzed within a task.
   * @param function function to flatmap, of type (state, sliding windows) -> (new state, result data)
   * @tparam T result data type
   * @tparam S state type
   * @return RDD[T] of flatmap results
   */
  def windowFlatMapWithState[M <: HasReferenceRegion: ClassTag, T: ClassTag, S](
    regionRDDs: PerSample[RDD[M]],
    lociPartitions: LociMap[Long],
    skipEmpty: Boolean,
    halfWindowSize: Long,
    initialState: S,
    function: (S, PerSample[SlidingWindow[M]]) => (S, Iterator[T])): RDD[T] = {
    windowTaskFlatMapMultipleRDDs(
      regionRDDs,
      lociPartitions,
      halfWindowSize,
      (task, taskLoci, taskRegionsPerSample: PerSample[Iterator[M]]) => {
        collectByContig[M, T](
          taskRegionsPerSample,
          taskLoci,
          halfWindowSize,
          (loci, windows) => {
            val lociIterator = loci.iterator
            var lastState: S = initialState
            val builder = Vector.newBuilder[T]
            while (SlidingWindow.advanceMultipleWindows(windows, lociIterator, skipEmpty).isDefined) {
              val (state, elements) = function(lastState, windows)
              lastState = state
              builder ++= elements
            }
            builder.result.iterator
          }
        )
      }
    )
  }

  /**
   *
   * Computes an aggregate over each task and contig
   * The user specified aggFunction is used to accumulate a result starting with `initialValue`
   *
   * @param regionRDDs RDDs of reads, one per sample
   * @param lociPartitions loci to consider, partitioned into tasks
   * @param skipEmpty If True, empty windows (no regions within the window) will be skipped
   * @param halfWindowSize A window centered at locus = l will contain regions overlapping l +/- halfWindowSize
   * @param initialValue Initial value for aggregation
   * @param aggFunction Function to aggregate windows, folds the windows into the aggregate value so far
   * @tparam T Type of the aggregation value
   * @return Iterator[T], the aggregate values collected over contigs
   */
  def windowFoldLoci[M <: HasReferenceRegion: ClassTag, T: ClassTag](regionRDDs: PerSample[RDD[M]],
                                                                     lociPartitions: LociMap[Long],
                                                                     skipEmpty: Boolean,
                                                                     halfWindowSize: Long,
                                                                     initialValue: T,
                                                                     aggFunction: (T, PerSample[SlidingWindow[M]]) => T): RDD[T] = {
    windowTaskFlatMapMultipleRDDs(
      regionRDDs,
      lociPartitions,
      halfWindowSize,
      (task, taskLoci, taskRegionsPerSample: PerSample[Iterator[M]]) => {
        collectByContig[M, T](
          taskRegionsPerSample,
          taskLoci,
          halfWindowSize,
          (loci, windows) => {
            val lociIterator = loci.iterator
            var value = initialValue
            while (SlidingWindow.advanceMultipleWindows(windows, lociIterator, skipEmpty).isDefined) {
              value = aggFunction(value, windows)
            }
            Iterator.single(value)
          })
      }
    )
  }

  /**
   *
   * Generates a sequence of results from each task (using the `generateFromWindows` function)
   * and collects them into a single iterator
   *
   * @param taskRegionsPerSample for each sample, elements of type M to process for this task
   * @param taskLoci Set of loci to process for this task
   * @param halfWindowSize A window centered at locus = l will contain regions overlapping l +/- halfWindowSize
   * @param generateFromWindows Function that maps windows to result type
   * @tparam T result data type
   * @return Iterator[T] collected from each contig
   */
  def collectByContig[M <: HasReferenceRegion: ClassTag, T: ClassTag](
    taskRegionsPerSample: PerSample[Iterator[M]],
    taskLoci: LociSet,
    halfWindowSize: Long,
    generateFromWindows: (LociSet.SingleContig, PerSample[SlidingWindow[M]]) => Iterator[T]): Iterator[T] = {

    val regionSplitByContigPerSample: PerSample[RegionsByContig[M]] = taskRegionsPerSample.map(new RegionsByContig(_))

    taskLoci.contigs.flatMap(contig => {
      val regionIterator: PerSample[Iterator[M]] = regionSplitByContigPerSample.map(_.next(contig))
      val windows: PerSample[SlidingWindow[M]] = regionIterator.map(SlidingWindow[M](contig, halfWindowSize, _))
      generateFromWindows(taskLoci.onContig(contig), windows)
    }).iterator
  }

  /**
   * Spark partitioner for keyed RDDs that assigns each unique key its own partition.
   * Used to partition an RDD of (task number: Long, read: MappedRead) pairs, giving each task its own partition.
   *
   * @param partitions total number of partitions
   */
  class PartitionByKey(partitions: Int) extends Partitioner {
    def numPartitions = partitions
    def getPartition(key: Any): Int = key match {
      case value: Long         => value.toInt
      case value: TaskPosition => value.task
      case _                   => throw new AssertionError("Unexpected key in PartitionByTask")
    }
    override def equals(other: Any): Boolean = other match {
      case h: PartitionByKey => h.numPartitions == numPartitions
      case _                 => false
    }
  }

  /**
   * TaskPosition represents the task a read is assigned to and the start position on the reference genome of the read
   * Each read is assigned to a task and the reads are sorted by (referenceContig, locus) when they are processed
   *
   * @param task Task ID
   * @param referenceContig Reference or chromosome name for reads
   * @param locus The position in the reference contig at which the read starts
   */
  case class TaskPosition(task: Int, referenceContig: String, locus: Long) extends Ordered[TaskPosition] {

    // Sorting is performed by first sorting on task, secondly on contig and lastly on the start locus
    override def compare(other: TaskPosition): Int = {
      if (task != other.task) {
        task - other.task
      } else {
        val contigComparison = referenceContig.compare(other.referenceContig)
        if (contigComparison != 0) {
          contigComparison
        } else {
          (locus - other.locus).toInt
        }
      }
    }
  }

  /**
   * FlatMap across sets of regions (e.g. reads) overlapping genomic partitions, on multiple RDDs.
   *
   * This function works as follows:
   *
   *  (1) Assign regions to partitions. A region may overlap multiple partitions, and therefore be assigned to multiple
   *      partitions.
   *
   *  (2) For each partition, call the provided function. The arguments to this function are the task number, the loci
   *      assigned to this task, and a sequence of iterators giving the regions overlapping those loci (within the
   *      specified halfWindowSize) from each corresponding input RDD. The loci assigned to this task are always unique
   *      to this task, but the same regions may be provided to multiple tasks, since regions may overlap loci partition
   *      boundaries.
   *
   *  (3) The results of the provided function are concatenated into an RDD, which is returned.
   *
   * @param regionRDDs RDDs of reads, one per sample.
   * @param lociPartitions map from locus -> task number. This argument specifies both the loci to be considered and how
   *                       they should be split among tasks. regions that don't overlap these loci are discarded.
   * @param halfWindowSize if a region overlaps a region of halfWindowSize to either side of a locus under consideration,
   *                       then it is included.
   * @param function function to flatMap: (task number, loci, iterators of regions that overlap a window around these
   *                 loci (one region-iterator per sample)) -> T
   * @tparam T type of value returned by function
   * @return flatMap results, RDD[T]
   */
  private def windowTaskFlatMapMultipleRDDs[M <: HasReferenceRegion: ClassTag, T: ClassTag](
    regionRDDs: PerSample[RDD[M]],
    lociPartitions: LociMap[Long],
    halfWindowSize: Long,
    // TODO(ryan): factor this function type out (as a PartialFunction?)
    function: (Long, LociSet, PerSample[Iterator[M]]) => Iterator[T]): RDD[T] = {

    val numRDDs = regionRDDs.length
    assume(numRDDs > 0)
    val sc = regionRDDs(0).sparkContext
    progress("Loci partitioning: %s".format(lociPartitions.truncatedString()))
    val lociPartitionsBoxed: Broadcast[LociMap[Long]] = sc.broadcast(lociPartitions)
    val numTasks = lociPartitions.asInverseMap.map(_._1).max + 1

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

    // Expand regions into (task, region) pairs for each region RDD.
    val taskNumberRegionPairsRDDs: PerSample[RDD[(TaskPosition, M)]] =
      regionRDDs.map(_.flatMap(region => {
        val singleContig = lociPartitionsBoxed.value.onContig(region.referenceContig)
        val thisRegionsTasks = singleContig.getAll(region.start - halfWindowSize, region.end + halfWindowSize)

        // Update counters
        totalRegions += 1
        if (thisRegionsTasks.nonEmpty) relevantRegions += 1
        expandedRegions += thisRegionsTasks.size

        // Return this region, duplicated for each task it is assigned to.
        thisRegionsTasks.map(task => (TaskPosition(task.toInt, region.referenceContig, region.start), region))
      }))

    // Run the task on each partition. Keep track of the number of regions assigned to each task in an accumulator, so
    // we can print out a summary of the skew.
    val regionsByTask = sc.accumulator(MutableHashMap.empty[String, Long])(new HashMapAccumulatorParam)
    DelayedMessages.default.say { () =>
      {
        val stats = new math3.stat.descriptive.DescriptiveStatistics()
        regionsByTask.value.valuesIterator.foreach(stats.addValue(_))
        "Regions per task: min=%,.0f 25%%=%,.0f median=%,.0f (mean=%,.0f) 75%%=%,.0f max=%,.0f. Max is %,.2f%% more than mean.".format(
          stats.getMin,
          stats.getPercentile(25),
          stats.getPercentile(50),
          stats.getMean,
          stats.getPercentile(75),
          stats.getMax,
          ((stats.getMax - stats.getMean) * 100.0) / stats.getMean)
      }
    }

    // Accumulator to track the number of loci in each task
    val lociAccumulator = sc.accumulator[Long](0, "NumLoci")

    // Build an RDD of (read set num, read), take union of this over all RDDs, and partition by task.
    val partitioned = sc.union(
      taskNumberRegionPairsRDDs.zipWithIndex.map({
        case (taskNumberRegionPairs, rddIndex: Int) => {
          taskNumberRegionPairs.map(pair => (pair._1, (rddIndex, pair._2)))
        }
      })).repartitionAndSortWithinPartitions(new PartitionByKey(numTasks.toInt)).map(_._2)

    partitioned.mapPartitionsWithIndex((taskNum, values) => {
      val iterators = SplitIterator.split(numRDDs, values)
      val taskLoci = lociPartitionsBoxed.value.asInverseMap(taskNum)
      lociAccumulator += taskLoci.count
      function(taskNum, taskLoci, iterators)
    })
  }

  /**
   * Using an iterator of regions sorted by (contig, start locus), this class exposes a way to get separate iterators
   * over the regions in each contig.
   *
   * For example, given these regions (written as contig:start locus):
   *    chr20:1000,chr20:1500,chr21:200
   *
   * Calling next("chr20") will return an iterator of two regions (chr20:1000 and chr20:1500). After that, calling
   * next("chr21") will give an iterator of one region (chr21:200).
   *
   * Note that you must call next("chr20") before calling next("chr21") in this example. That is, this class does not
   * buffer anything -- it just walks forward in the regions using the iterator you gave it.
   *
   * Also note that after calling next("chr21"), the iterator returned by our previous call to next() is invalidated.
   *
   * @param regionIterator regions, sorted by contig and start locus.
   */
  class RegionsByContig[Mapped <: HasReferenceRegion](regionIterator: Iterator[Mapped]) {
    private val buffered = regionIterator.buffered
    private var seenContigs = List.empty[String]
    private var prevIterator: Option[SingleContigRegionIterator[Mapped]] = None
    def next(contig: String): Iterator[Mapped] = {
      // We must first march the previous iterator we returned to the end.
      while (prevIterator.exists(_.hasNext)) prevIterator.get.next()

      // The next element from the iterator should have a contig we haven't seen so far.
      assert(buffered.isEmpty || !seenContigs.contains(buffered.head.referenceContig),
        "Regions are not sorted by contig. Contigs requested so far: %s. Next regions's contig: %s.".format(
          seenContigs.reverse.toString, buffered.head.referenceContig))
      seenContigs ::= contig

      // Wrap our iterator and return it.
      prevIterator = Some(new SingleContigRegionIterator(contig, buffered))
      prevIterator.get
    }
  }

  /**
   * Wraps an iterator of regions sorted by contig name. Implements an iterator that gives regions only for the specified
   * contig name, then stops.
   */
  class SingleContigRegionIterator[Mapped <: HasReferenceRegion](contig: String, iterator: BufferedIterator[Mapped]) extends Iterator[Mapped] {
    def hasNext = iterator.hasNext && iterator.head.referenceContig == contig
    def next() = if (hasNext) iterator.next() else throw new NoSuchElementException
  }

  /**
   * Allows a mutable HashMap[String, Long] to be used as an accumulator in Spark.
   *
   * When we put (k, v2) into an accumulator that already contains (k, v1), the result will be a HashMap containing
   * (k, v1 + v2).
   *
   */
  class HashMapAccumulatorParam extends AccumulatorParam[MutableHashMap[String, Long]] {
    /**
     * Combine two accumulators. Adds the values in each hash map.
     *
     * This method is allowed to modify and return the first value for efficiency.
     *
     * @see org.apache.spark.GrowableAccumulableParam.addInPlace(r1: R, r2: R): R
     *
     */
    def addInPlace(first: MutableHashMap[String, Long], second: MutableHashMap[String, Long]): MutableHashMap[String, Long] = {
      second.foreach(pair => {
        if (!first.contains(pair._1))
          first(pair._1) = pair._2
        else
          first(pair._1) += pair._2
      })
      first
    }

    /**
     * Zero value for the accumulator: the empty hash map.
     *
     * @see org.apache.spark.GrowableAccumulableParam.zero(initialValue: R): R
     *
     */
    def zero(initialValue: MutableHashMap[String, Long]): MutableHashMap[String, Long] = {
      val ser = new JavaSerializer(new SparkConf(false)).newInstance()
      val copy = ser.deserialize[MutableHashMap[String, Long]](ser.serialize(initialValue))
      copy.clear()
      copy
    }
  }
}
