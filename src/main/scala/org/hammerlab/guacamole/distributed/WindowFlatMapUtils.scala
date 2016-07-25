package org.hammerlab.guacamole.distributed

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.loci.partitioning.LociPartitioning
import org.hammerlab.guacamole.loci.set.{Contig, LociSet}
import org.hammerlab.guacamole.readsets.PerSample
import org.hammerlab.guacamole.readsets.rdd.PartitionedRegions
import org.hammerlab.guacamole.reference.ReferenceRegion
import org.hammerlab.guacamole.windowing.{SlidingWindow, SplitIterator}

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
  def windowFlatMapWithState[R <: ReferenceRegion: ClassTag, T: ClassTag, S](regionRDDs: PerSample[RDD[R]],
                                                                             lociPartitions: LociPartitioning,
                                                                             skipEmpty: Boolean,
                                                                             halfWindowSize: Int,
                                                                             initialState: S,
                                                                             function: (S, PerSample[SlidingWindow[R]]) => (S, Iterator[T])): RDD[T] = {
    windowTaskFlatMapMultipleRDDs(
      regionRDDs,
      lociPartitions,
      halfWindowSize,
      (_, partitionLoci, taskRegionsPerSample: PerSample[Iterator[R]]) => {
        collectByContig[R, T](
          taskRegionsPerSample,
          partitionLoci,
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
  def windowFoldLoci[R <: ReferenceRegion: ClassTag, T: ClassTag](regionRDDs: PerSample[RDD[R]],
                                                                  lociPartitions: LociPartitioning,
                                                                  skipEmpty: Boolean,
                                                                  halfWindowSize: Int,
                                                                  initialValue: T,
                                                                  aggFunction: (T, PerSample[SlidingWindow[R]]) => T): RDD[T] = {
    windowTaskFlatMapMultipleRDDs(
      regionRDDs,
      lociPartitions,
      halfWindowSize,
      (task, partitionLoci, taskRegionsPerSample: PerSample[Iterator[R]]) => {
        collectByContig[R, T](
          taskRegionsPerSample,
          partitionLoci,
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
   * @param halfWindowSize if a region overlaps a region of halfWindowSize to either side of a locus under
   *                       consideration, then it is included.
   * @param function function to flatMap: (task number, loci, iterators of regions that overlap a window around these
   *                 loci (one region-iterator per sample)) -> T
   * @tparam T type of value returned by function
   * @return flatMap results, RDD[T]
   */
  private[distributed] def windowTaskFlatMapMultipleRDDs[R <: ReferenceRegion: ClassTag, T: ClassTag](
    regionRDDs: PerSample[RDD[R]],
    lociPartitions: LociPartitioning,
    halfWindowSize: Int,
    function: (Long, LociSet, PerSample[Iterator[R]]) => Iterator[T]): RDD[T] = {

    val numRDDs = regionRDDs.length
    assume(numRDDs > 0)

    val sc = regionRDDs(0).sparkContext

    val lociPartitionsBoxed: Broadcast[LociPartitioning] = sc.broadcast(lociPartitions)

    val partitionedRegions = PartitionedRegions(regionRDDs, lociPartitionsBoxed, halfWindowSize)

    // Accumulator to track the number of loci in each task
    val lociAccumulator = sc.accumulator[Long](0, "NumLoci")

    partitionedRegions
      .mapPartitionsWithIndex((partitionIdx, keyedReads) => {
        val iterators = SplitIterator.split(numRDDs, keyedReads)
        val partitionLoci = lociPartitionsBoxed.value.inverse(partitionIdx)
        lociAccumulator += partitionLoci.count
        function(partitionIdx, partitionLoci, iterators)
      })
  }

  /**
   *
   * Generates a sequence of results from each task (using the `generateFromWindows` function)
   * and collects them into a single iterator
   *
   * @param taskRegionsPerSample for each sample, elements of type M to process for this task
   * @param partitionLoci Set of loci to process for this task
   * @param halfWindowSize A window centered at locus = l will contain regions overlapping l +/- halfWindowSize
   * @param generateFromWindows Function that maps windows to result type
   * @tparam T result data type
   * @return Iterator[T] collected from each contig
   */
  def collectByContig[R <: ReferenceRegion: ClassTag, T: ClassTag](
    taskRegionsPerSample: PerSample[Iterator[R]],
    partitionLoci: LociSet,
    halfWindowSize: Int,
    generateFromWindows: (Contig, PerSample[SlidingWindow[R]]) => Iterator[T]): Iterator[T] = {

    val regionSplitByContigPerSample: PerSample[RegionsByContig[R]] = taskRegionsPerSample.map(new RegionsByContig(_))

    partitionLoci.contigs.flatMap(contig => {
      val regionIterator: PerSample[Iterator[R]] = regionSplitByContigPerSample.map(_.next(contig.name))
      val windows: PerSample[SlidingWindow[R]] = regionIterator.map(SlidingWindow[R](contig.name, halfWindowSize, _))
      generateFromWindows(contig, windows)
    }).iterator
  }
}
