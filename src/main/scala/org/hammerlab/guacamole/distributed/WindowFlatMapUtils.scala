package org.hammerlab.guacamole.distributed

import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.loci.set.{LociIterator, LociSet}
import org.hammerlab.guacamole.reads.SampleRegion
import org.hammerlab.guacamole.readsets.rdd.PartitionedRegions
import org.hammerlab.guacamole.readsets.{NumSamples, PerSample}
import org.hammerlab.guacamole.reference.ReferenceRegion
import org.hammerlab.guacamole.windowing.{SlidingWindow, SplitIterator}

import scala.reflect.ClassTag

object WindowFlatMapUtils {

  /**
   * FlatMap across loci, and any number of RDDs of regions, where at each locus the provided function is passed a
   * sliding window instance for each RDD containing the regions overlapping an interval of halfWindowSize to either
   * side of a locus.
   *
   * This function supports maintaining some state from one locus to another within a task. The state maintained is of
   * type S. The user function will receive the current state in addition to the sliding windows, and returns a pair of
   * (new state, result data). The state is initialized to initialState for each task, and for each new contig handled
   * by a single task.
   *
   * @param numSamples number of samples / input-files whose reads are in @partitionedReads.
   * @param partitionedReads partitioned reads RDD; reads that straddle partition boundaries will occur more than once
   *                         herein.
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
  def windowFlatMapWithState[R <: SampleRegion: ClassTag, T: ClassTag, S](
    numSamples: NumSamples,
    partitionedReads: PartitionedRegions[R],
    skipEmpty: Boolean,
    halfWindowSize: Int,
    initialState: S,
    function: (S, PerSample[SlidingWindow[R]]) => (S, Iterator[T])): RDD[T] = {

    splitSamplesAndMap(
      numSamples,
      partitionedReads,
      (partitionLoci, taskRegionsPerSample: PerSample[Iterator[R]]) => {
        splitPartitionByContigAndMap[R, T](
          taskRegionsPerSample,
          partitionLoci,
          halfWindowSize,
          (contigLoci, perSampleWindows) => {

            var lastState: S = initialState

            // Accumulates results in a Vector before returning an Iterator; some benchmarking suggested this to be
            // faster, cf. https://github.com/hammerlab/guacamole/issues/386#issuecomment-198754370, but that is
            // probably worth revisiting.
            val builder = Vector.newBuilder[T]

            while (SlidingWindow.advanceMultipleWindows(perSampleWindows, contigLoci, skipEmpty).isDefined) {
              val (state, elements) = function(lastState, perSampleWindows)
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
   * Map over partitioned reads, splitting them by logical sample.
   *
   * For each partition, pass that partition's loci and regions (split by sample) to `function`. The loci assigned
   * processed in a given partition are always unique to that partition, but the same regions may be processed by
   * multiple partitions, since regions may overlap loci partition boundaries.
   *
   * @param numSamples number of samples / input-files whose reads are in `partitionedReads`.
   * @param partitionedReads partitioned reads; reads that straddle partition boundaries will occur more than once
   *                         herein.
   * @param function function to apply: (loci, iterators of regions that overlap a window around these loci (one
   *                 region-iterator per sample)) -> [[Iterator[T]]]
   * @tparam T type of returned [[RDD]]
   * @return [[RDD[T]]]
   */
  private[distributed] def splitSamplesAndMap[R <: SampleRegion: ClassTag, T: ClassTag](
    numSamples: NumSamples,
    partitionedReads: PartitionedRegions[R],
    function: (LociSet, PerSample[Iterator[R]]) => Iterator[T]): RDD[T] = {

    partitionedReads
      .mapPartitions(
        (reads, loci) =>
          function(
            loci,
            SplitIterator.split[R](numSamples, reads, _.sampleId)
          )
      )
  }

  /**
   * For a given partition, step through its loci and the reads overlapping each one, applying an arbitrary function and
   * returning its emitted objects.
   *
   * @param perSampleTaskRegions this partition's regions, split by sample
   * @param partitionLoci this partition's loci
   * @param halfWindowSize a margin within which reads are considered to effectively overlap a locus
   * @param generateFromWindows function that maps a contig's loci and regions to a sequence of result objects.
   * @tparam T result data type
   * @return Iterator[T] collected from each contig
   */
  def splitPartitionByContigAndMap[R <: ReferenceRegion: ClassTag, T: ClassTag](
    perSampleTaskRegions: PerSample[Iterator[R]],
    partitionLoci: LociSet,
    halfWindowSize: Int,
    generateFromWindows: (LociIterator, PerSample[SlidingWindow[R]]) => Iterator[T]): Iterator[T] = {

    val perSampleRegionsByContig: PerSample[RegionsByContig[R]] =
      perSampleTaskRegions.map(new RegionsByContig(_))

    // NOTE: we rely here on the reads having been sorted lexicographically by contig-name in the
    // repartitionAndSortWithinPartitions above, and also in LociSet.contigs.
    for {
      // For each contig…
      contigLoci <- partitionLoci.contigs.iterator

      // For each sample, an iterator of regions on this contig.
      perSampleContigRegions: PerSample[Iterator[R]] = perSampleRegionsByContig.map(_.next(contigLoci.name))

      // For each sample, an advanceable window of regions overlapping sequential loci, with a grace-margin of
      // `halfWindowSize`.
      windows: PerSample[SlidingWindow[R]] = perSampleContigRegions.map(SlidingWindow(contigLoci.name, halfWindowSize, _))

      // Pass this contig's loci and per-sample "windows" to the supplied closure, and emit each resulting object.
      t <- generateFromWindows(contigLoci.iterator, windows)
    } yield
      t
  }
}
