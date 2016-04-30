package org.hammerlab.guacamole.distributed

import org.apache.commons.math3
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.loci.map.LociMap
import org.hammerlab.guacamole.loci.set.{Contig, LociSet}
import org.hammerlab.guacamole.logging.DelayedMessages
import org.hammerlab.guacamole.logging.LoggingUtils.progress
import org.hammerlab.guacamole.windowing.{SlidingWindow, SplitIterator}
import org.hammerlab.guacamole.PerSample
import org.hammerlab.guacamole.reference.Region

import scala.collection.mutable.{HashMap => MutableHashMap}
import scala.reflect.ClassTag

object WindowFlatMapUtils {

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
    * @tparam R region data type (e.g. MappedRead)
    * @tparam T result data type
    * @tparam S state type
    * @return RDD[T] of flatmap results
    */
  def windowFlatMapWithState[R <: Region: ClassTag, T: ClassTag, S](regionRDDs: PerSample[RDD[R]],
                                                                    lociPartitions: LociMap[Long],
                                                                    skipEmpty: Boolean,
                                                                    halfWindowSize: Long,
                                                                    initialState: S,
                                                                    function: (S, PerSample[SlidingWindow[R]]) => (S, Iterator[T])): RDD[T] = {
    windowTaskFlatMapMultipleRDDs(
      regionRDDs,
      lociPartitions,
      halfWindowSize,
      (task, taskLoci, taskRegionsPerSample: PerSample[Iterator[R]]) => {
        collectByContig[R, T](
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
  def windowFoldLoci[R <: Region: ClassTag, T: ClassTag](regionRDDs: PerSample[RDD[R]],
                                                         lociPartitions: LociMap[Long],
                                                         skipEmpty: Boolean,
                                                         halfWindowSize: Long,
                                                         initialValue: T,
                                                         aggFunction: (T, PerSample[SlidingWindow[R]]) => T): RDD[T] = {
    windowTaskFlatMapMultipleRDDs(
      regionRDDs,
      lociPartitions,
      halfWindowSize,
      (task, taskLoci, taskRegionsPerSample: PerSample[Iterator[R]]) => {
        collectByContig[R, T](
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
  private[distributed] def windowTaskFlatMapMultipleRDDs[R <: Region: ClassTag, T: ClassTag](
    regionRDDs: PerSample[RDD[R]],
    lociPartitions: LociMap[Long],
    halfWindowSize: Long,
    function: (Long, LociSet, PerSample[Iterator[R]]) => Iterator[T]): RDD[T] = {

    val numRDDs = regionRDDs.length
    assume(numRDDs > 0)
    val sc = regionRDDs(0).sparkContext
    progress(s"Loci partitioning: $lociPartitions")
    val lociPartitionsBoxed: Broadcast[LociMap[Long]] = sc.broadcast(lociPartitions)
    val numTasks = lociPartitions.inverse.map(_._1).max + 1

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
    val taskNumberRegionPairsRDDs: PerSample[RDD[(TaskPosition, R)]] =
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
    DelayedMessages.default.say {
      () => {
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
      })).repartitionAndSortWithinPartitions(new KeyPartitioner(numTasks.toInt)).map(_._2)

    partitioned.mapPartitionsWithIndex((taskNum, values) => {
      val iterators = SplitIterator.split(numRDDs, values)
      val taskLoci = lociPartitionsBoxed.value.inverse(taskNum)
      lociAccumulator += taskLoci.count
      function(taskNum, taskLoci, iterators)
    })
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
  def collectByContig[R <: Region: ClassTag, T: ClassTag](
    taskRegionsPerSample: PerSample[Iterator[R]],
    taskLoci: LociSet,
    halfWindowSize: Long,
    generateFromWindows: (Contig, PerSample[SlidingWindow[R]]) => Iterator[T]): Iterator[T] = {

    val regionSplitByContigPerSample: PerSample[RegionsByContig[R]] = taskRegionsPerSample.map(new RegionsByContig(_))

    taskLoci.contigs.flatMap(contig => {
      val regionIterator: PerSample[Iterator[R]] = regionSplitByContigPerSample.map(_.next(contig.name))
      val windows: PerSample[SlidingWindow[R]] = regionIterator.map(SlidingWindow[R](contig.name, halfWindowSize, _))
      generateFromWindows(contig, windows)
    }).iterator
  }
}
