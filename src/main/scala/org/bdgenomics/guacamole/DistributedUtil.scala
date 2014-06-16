package org.bdgenomics.guacamole

import org.bdgenomics.guacamole.Common._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.commons.math3
import org.apache.spark._
import scala.reflect.ClassTag
import org.bdgenomics.guacamole.Common.Arguments.{ Loci, Base }
import org.kohsuke.args4j.{ Option => Opt }
import org.bdgenomics.guacamole.pileup.Pileup
import scala.Some
import org.apache.spark.serializer.JavaSerializer
import scala.collection.mutable.{ HashMap => MutableHashMap, ArrayBuffer }

object DistributedUtil extends Logging {
  trait Arguments extends Base with Loci {
    @Opt(name = "-parallelism", usage = "Num variant calling tasks. Set to 0 (default) to use the number of Spark partitions.")
    var parallelism: Int = 0

    @Opt(name = "-partition-accuracy",
      usage = "Num micro partitions to use per task in loci partitioning. Set to 0 to partition loci uniformly. Default: 250.")
    var partitioningAccuracy: Int = 250
  }

  /**
   * Partition a LociSet among tasks according to the strategy specified in args.
   */
  def partitionLociAccordingToArgs(args: Arguments, loci: LociSet, readsRDDs: RDD[MappedRead]*): LociMap[Long] = {
    val tasks = if (args.parallelism > 0) args.parallelism else readsRDDs(0).partitions.length
    if (args.partitioningAccuracy == 0) {
      partitionLociUniformly(tasks, loci)
    } else {
      partitionLociByApproximateReadDepth(
        tasks,
        loci,
        args.partitioningAccuracy,
        readsRDDs: _*)
    }
  }

  /**
   * Assign loci from a LociSet to partitions. Contiguous intervals of loci will tend to get assigned to the same
   * partition.
   *
   * This implementation assigns loci uniformly, i.e. each task gets about the same number of loci. A smarter
   * implementation would know about the reads (depth of coverage), and try to assign each task loci corresponding to
   * about the same number of reads.
   *
   * @param tasks number of partitions
   * @param loci loci to partition
   * @return LociMap of locus -> task assignments
   */
  def partitionLociUniformly(tasks: Long, loci: LociSet): LociMap[Long] = {
    assume(tasks >= 1)
    val lociPerTask = math.max(1, loci.count.toDouble / tasks.toDouble)
    progress("Splitting loci evenly among %,d tasks = ~%,.0f loci per task".format(tasks, lociPerTask))
    val builder = LociMap.newBuilder[Long]
    var lociAssigned = 0L
    var task = 0L
    def remainingForThisTask = math.round(((task + 1) * lociPerTask) - lociAssigned).toLong
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
   * Given a LociSet and an RDD of reads, returns the same LociSet but with any contigs that don't have any reads
   * mapped to them removed. Also prints out progress info on the number of reads assigned to each contig.
   */
  def filterLociWhoseContigsHaveNoReads(loci: LociSet, reads: RDD[MappedRead]): LociSet = {
    val contigsAndCounts = reads.map(_.referenceContig).countByValue.toMap.withDefaultValue(0L)
    Common.progress("Read counts per contig: %s".format(
      contigsAndCounts.toSeq.sorted.map(pair => "%s=%,d".format(pair._1, pair._2)).mkString(" ")))
    val contigsWithoutReads = loci.contigs.filter(contigsAndCounts(_) == 0L).toSet
    if (contigsWithoutReads.nonEmpty) {
      Common.progress("Filtering out these contigs, since they have no reads: %s".format(
        contigsWithoutReads.toSeq.sorted.mkString(", ")))
      loci.filterContigs(!contigsWithoutReads.contains(_))
    } else {
      loci
    }
  }

  /**
   * Assign loci from a LociSet to partitions, where each partition overlaps about the same number of reads.
   *
   * The approach we take is:
   *
   *  (1) chop up the loci uniformly into many genomic "micro partitions."
   *
   *  (2) for each micro partition, calculate the number of reads that overlap it.
   *
   *  (3) using these counts, assign loci to real ("macro") partitions, making the approximation of uniform depth within
   *      each micro partition.
   *
   *  Some advantages of this approach are:
   *
   *  - Stages (1) and (3), which are done locally by the Spark master, are constant time with respect to the number
   *    of reads.
   *
   *  - Stage (2), where runtime does depend on the number of reads, is done in parallel with Spark.
   *
   *  - We can tune the accuracy vs. performance trade off by setting `microTasks`.
   *
   *  - Does not require a distributed sort.
   *
   * @param tasks number of partitions
   * @param loci loci to partition
   * @param accuracy integer >= 1. Higher values of this will result in a more exact but also more expensive computation.
   *                 Specifically, this is the number of micro partitions to use per task to estimate the read depth.
   *                 In the extreme case, setting this to greater than the number of loci per task will result in an
   *                 exact calculation.
   * @param readRDDs: reads RDD 1, reads RDD 2, ...
   *                Any number RDD[MappedRead] arguments giving the reads to base the partitioning on.
   * @return LociMap of locus -> task assignments.
   */
  def partitionLociByApproximateReadDepth(tasks: Int, loci: LociSet, accuracy: Int, readRDDs: RDD[MappedRead]*): LociMap[Long] = {
    assume(tasks >= 1)
    assume(readRDDs.length > 0)
    val sc = readRDDs(0).sparkContext

    // As an optimization for the case where some contigs have no reads, we remove contigs without reads first.
    val lociUsed = filterLociWhoseContigsHaveNoReads(loci, sc.union(readRDDs))
    assume(lociUsed.count > 0)

    val (numMicroPartitions, broadcastMicroPartitions) = splitIntoMicroPartitions(sc, tasks, lociUsed, accuracy, readRDDs: _*)
    val counts = calculateOverlapsPerMicroPartition(numMicroPartitions, broadcastMicroPartitions, readRDDs: _*)
    val result = assignLociToRealPartitions(tasks, broadcastMicroPartitions.value, numMicroPartitions, loci, lociUsed, counts)

    result
  }

  /**
   * Split loci uniformly into micro partitions.
   */
  private def splitIntoMicroPartitions(sc: SparkContext, tasks: Int, lociUsed: LociSet, accuracy: Int, readRDDs: RDD[MappedRead]*): (Int, Broadcast[LociMap[Long]]) = {
    val numMicroPartitions: Int = if (accuracy * tasks < lociUsed.count) accuracy * tasks else lociUsed.count.toInt
    progress("Splitting loci by read depth among %,d tasks using %,d micro partitions.".format(tasks, numMicroPartitions))
    val microPartitions = partitionLociUniformly(numMicroPartitions, lociUsed)
    progress("Done calculating micro partitions.")
    val broadcastMicroPartitions = sc.broadcast(microPartitions)

    (numMicroPartitions, broadcastMicroPartitions)
  }

  /**
   * Total up reads overlapping each micro partition. We keep the totals as an array of Longs.
   */
  private def calculateOverlapsPerMicroPartition(numMicroPartitions: Int, broadcastMicroPartitions: Broadcast[LociMap[Long]], readRDDs: RDD[MappedRead]*) = {
    def addArray(first: Array[Long], second: Array[Long]): Array[Long] = {
      assert(first.length == second.length)
      val result = new Array[Long](first.length)
      var i = 0
      while (i < first.length) {
        result(i) = first(i) + second(i)
        i += 1
      }
      result
    }

    var num = 1
    val counts = readRDDs.map(reads => {
      progress("Collecting read counts for RDD %d of %d.".format(num, readRDDs.length))
      num += 1
      reads.mapPartitions(readIterator => {
        val microPartitions = broadcastMicroPartitions.value
        assert(microPartitions.count > 0)
        val counts = new Array[Long](numMicroPartitions)
        readIterator.foreach(read => {
          val contigMap = microPartitions.onContig(read.referenceContig)
          val indices: Set[Long] = contigMap.getAll(read.start, read.end)
          indices.foreach(index => {
            counts(index.toInt) += 1
          })
        })
        Seq(counts).iterator
      }).reduce(addArray _)
    }).reduce(addArray _)

    counts
  }

  /**
   * Assign loci to tasks, taking into account read depth in each micro partition.
   */
  private def assignLociToRealPartitions(tasks: Int, microPartitions: LociMap[Long], numMicroPartitions: Int, loci: LociSet, lociUsed: LociSet, counts: Array[Long]): LociMap[Long] = {
    val totalReads = counts.sum
    val readsPerTask = math.max(1, totalReads.toDouble / tasks.toDouble)
    progress("Done collecting read counts. Total reads with micro partition overlaps: %,d = ~%,.0f reads per task."
      .format(totalReads, readsPerTask))
    progress("Reads per micro partition: min=%,d mean=%,.0f max=%,d.".format(
      counts.min, counts.sum.toDouble / counts.length, counts.max))

    val builder = LociMap.newBuilder[Long]
    builder.put(loci.filterContigs(!lociUsed.contigs.contains(_)), 0) // Empty contigs get assigned to task 0.
    var readsAssigned = 0.0
    var task = 0L
    def readsRemainingForThisTask = math.round(((task + 1) * readsPerTask) - readsAssigned).toLong
    var microTask = 0
    while (microTask < numMicroPartitions) {
      var set = microPartitions.asInverseMap(microTask)
      var readsInSet = counts(microTask)
      while (!set.isEmpty) {
        if (readsInSet == 0) {
          // Take the whole set if there are no reads assigned to it.
          builder.put(set, task)
          set = LociSet.empty
        } else {
          // If we've allocated all reads for this task, move on to the next task.
          if (readsRemainingForThisTask == 0)
            task += 1
          assert(readsRemainingForThisTask > 0)
          assert(task < tasks)

          // Making the approximation of uniform depth within each micro partition, we assign a proportional number of
          // loci and reads to the current task. The proportion of loci we assign is the ratio of how many reads we have
          // remaining to allocate for the current task vs. how many reads are remaining in the current micro partition.

          // Here we calculate the fraction of the current micro partition we are going to assign to the current task.
          // May be 1.0, in which case all loci (and therefore reads) for this micro partition will be assigned to the
          // current task.
          val fractionToTake = math.min(1.0, readsRemainingForThisTask.toDouble / readsInSet.toDouble)

          // Based on fractionToTake, we set the number of loci and reads to assign.
          // We always take at least 1 locus to ensure we continue to make progress.
          val lociToTake = math.max(1, (fractionToTake * set.count).toLong)
          val readsToTake = (fractionToTake * readsInSet).toLong

          // Add the new task assignment to the builder, and update bookkeeping info.
          val (currentSet, remainingSet) = set.take(lociToTake)
          builder.put(currentSet, task)
          readsAssigned += math.round(readsToTake).toLong
          readsInSet -= math.round(readsToTake).toLong
          set = remainingSet
        }
      }
      microTask += 1
    }
    val result = builder.result
    assert(result.count == loci.count)

    result
  }

  /**
   * Helper function. Given optionally an existing Pileup, a sliding read window, and a locus:
   *   - advance the window to the new locus.
   *   - return a new Pileup at the given locus. If an existing Pileup is given as input, then the result will share
   *     elements with that Pileup for efficiency.
   *
   *  If an existing Pileup is provided, then its locus must be <= the new locus.
   */
  private def initOrMovePileup(existing: Option[Pileup], window: SlidingReadWindow, locus: Long): Pileup = {
    val newReads = window.setCurrentLocus(locus)
    existing match {
      case None         => Pileup(newReads, locus)
      case Some(pileup) => pileup.atGreaterLocus(locus, newReads.iterator)
    }
  }

  /**
   * Helper function. Given some sliding window instances, return the lowest nextStartLocus from any of them. If all of
   * the sliding windows are at the end of the read iterators, return Long.MaxValue.
   */
  def firstStartLocus(windows: SlidingReadWindow*) = {
    windows.map(_.nextStartLocus.getOrElse(Long.MaxValue)).min
  }

  /**
   * Flatmap across loci, where at each locus the provided function is passed a Pileup instance.
   *
   * @param skipEmpty If true, the function will only be called at loci that have nonempty pileups, i.e. those
   *                  where at least one read overlaps. If false, then the function will be called at all the
   *                  specified loci. In cases where whole contigs may have no reads mapped (e.g. if running on
   *                  only a single chromosome, but loading loci from a sequence dictionary that includes the
   *                  entire genome), this is an important optimization.
   *
   * See [[windowTaskFlatMapMultipleRDDs()]] for other argument descriptions.
   *
   */
  def pileupFlatMap[T: ClassTag](reads: RDD[MappedRead],
                                 lociPartitions: LociMap[Long],
                                 skipEmpty: Boolean,
                                 function: Pileup => Iterator[T]): RDD[T] = {
    windowTaskFlatMap(reads, lociPartitions, 0L, (task, taskLoci, taskReads) => {
      // The code here is running in parallel in each task.

      val readsSplitByContig = new ReadsByContig(taskReads)

      // For each contig, we loop over its ranges, and accumulate the results of calling the user's function.
      // If skipEmpty=True and the pileup is empty at a locus, then we fast forward to the next locus with a mapped read.
      val result = new ArrayBuffer[T]
      taskLoci.contigs.foreach(contig => {
        val readsIterator = readsSplitByContig.next(contig)
        val window = SlidingReadWindow(0L, readsIterator)
        var maybePileup: Option[Pileup] = None
        def pileupEmpty = maybePileup.forall(_.elements.isEmpty)
        var locus: Long = 0L
        val ranges = taskLoci.onContig(contig).ranges.iterator
        while (ranges.hasNext && locus < Long.MaxValue) {
          val range = ranges.next()
          locus = math.max(locus, range.start)
          while (locus < range.end) {
            lazy val nextLocusWithReads = firstStartLocus(window)
            if (skipEmpty && pileupEmpty && nextLocusWithReads > locus) {
              // Fast forward.
              locus = nextLocusWithReads
            } else {
              // Run at this locus.
              maybePileup = Some(initOrMovePileup(maybePileup, window, locus))
              if (!(skipEmpty && pileupEmpty))
                result ++= function(maybePileup.get)
              locus += 1
            }
          }
        }
      })
      result.iterator
    })
  }
  /**
   * Flatmap across loci on two RDDs of MappedReads. At each locus the provided function is passed two Pileup instances,
   * giving the pileup for the reads in each RDD at that locus.
   *
   * @param skipEmpty see [[pileupFlatMap]] for description.
   *
   * See [[windowTaskFlatMapMultipleRDDs()]] for other argument descriptions.
   *
   */
  def pileupFlatMapTwoRDDs[T: ClassTag](reads1: RDD[MappedRead],
                                        reads2: RDD[MappedRead],
                                        lociPartitions: LociMap[Long],
                                        skipEmpty: Boolean,
                                        function: (Pileup, Pileup) => Iterator[T]): RDD[T] = {
    windowTaskFlatMapTwoRDDs(reads1, reads2, lociPartitions, 0L, (task, taskLoci, taskReads1, taskReads2) => {
      // The code here is running in parallel in each task.

      // A map from contig to iterator over reads that are in that contig:
      val readsSplitByContig1 = new ReadsByContig(taskReads1)
      val readsSplitByContig2 = new ReadsByContig(taskReads2)

      val result = new ArrayBuffer[T]
      taskLoci.contigs.foreach(contig => {
        val readsIterator1 = readsSplitByContig1.next(contig)
        val readsIterator2 = readsSplitByContig2.next(contig)
        val window1 = SlidingReadWindow(0L, readsIterator1)
        val window2 = SlidingReadWindow(0L, readsIterator2)
        var maybePileup1: Option[Pileup] = None
        var maybePileup2: Option[Pileup] = None
        def pileupsEmpty = maybePileup1.forall(_.elements.isEmpty) && maybePileup2.forall(_.elements.isEmpty)
        var locus: Long = 0L
        val ranges = taskLoci.onContig(contig).ranges.iterator
        while (ranges.hasNext && locus < Long.MaxValue) {
          val range = ranges.next()
          locus = math.max(locus, range.start)
          while (locus < range.end) {
            lazy val nextLocusWithReads = firstStartLocus(window1, window2)
            if (skipEmpty && pileupsEmpty && nextLocusWithReads > locus) {
              // Fast forward.
              locus = nextLocusWithReads
            } else {
              // Run at this locus.
              maybePileup1 = Some(initOrMovePileup(maybePileup1, window1, locus))
              maybePileup2 = Some(initOrMovePileup(maybePileup2, window2, locus))
              if (!(skipEmpty && pileupsEmpty))
                result ++= function(maybePileup1.get, maybePileup2.get)
              locus += 1
            }
          }
        }
        while (readsIterator1.hasNext) readsIterator1.next()
        while (readsIterator2.hasNext) readsIterator2.next()
      })
      result.iterator
    })
  }

  /**
   * FlatMap across loci, where at each locus the provided function is passed a SlidingWindow instance containing reads
   * overlapping that locus with the specified halfWindowSize.
   *
   * Currently unused, but here for demonstration.
   *
   * See [[windowTaskFlatMapMultipleRDDs()]] for argument descriptions.
   *
   */
  def windowFlatMap[T: ClassTag](reads: RDD[MappedRead],
                                 lociPartitions: LociMap[Long],
                                 halfWindowSize: Long,
                                 function: SlidingReadWindow => Iterator[T]): RDD[T] = {
    windowTaskFlatMap(reads, lociPartitions, halfWindowSize, (task, taskLoci, taskReads) => {
      val readsSplitByContig = new ReadsByContig(taskReads)
      val result = new ArrayBuffer[T]
      taskLoci.contigs.foreach(contig => {
        val readsIterator = readsSplitByContig.next(contig)
        val window = SlidingReadWindow(halfWindowSize, readsIterator)
        val ranges = taskLoci.onContig(contig).ranges.iterator
        while (ranges.hasNext) {
          val range = ranges.next()
          var locus = range.start
          while (locus < range.end) {
            window.setCurrentLocus(locus)
            result ++= function(window)
            locus += 1
          }
        }
      })
      result.iterator
    })
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
      case value: Long => value.toInt
      case _           => throw new AssertionError("Unexpected key in PartitionByTask")
    }
    override def equals(other: Any): Boolean = other match {
      case h: PartitionByKey => h.numPartitions == numPartitions
      case _                 => false
    }
  }

  /**
   * FlatMap across sets of reads overlapping genomic partitions.
   *
   * The provided function will be called once per task, and passed the following arguments:
   *  - the task number
   *  - the loci assigned to this task
   *  - an iterator over reads that overlap these loci (within some window size)
   *
   * The results of the function are concatenated into an RDD, which is returned.
   *
   * See [[windowTaskFlatMapMultipleRDDs]] for more details.
   *
   * @param reads sorted mapped reads
   * @param lociPartitions map from locus -> task number. This argument specifies both the loci to be considered and how
   *                       they should be split among tasks. Reads that don't overlap these loci are ignored.
   * @param halfWindowSize if a read overlaps a region of halfWindowSize to either side of a locus under consideration,
   *                       then it is included.
   * @param function function to flatMap: (task number, loci, reads that overlap a window around these loci) -> T
   * @tparam T type of value returned by function
   * @return flatMap results, RDD[T]
   */
  def windowTaskFlatMap[T: ClassTag](reads: RDD[MappedRead],
                                     lociPartitions: LociMap[Long],
                                     halfWindowSize: Long,
                                     function: (Long, LociSet, Iterator[MappedRead]) => Iterator[T]): RDD[T] = {
    windowTaskFlatMapMultipleRDDs(Seq(reads), lociPartitions, halfWindowSize, (locus, loci, iterators) => {
      assert(iterators.length == 1)
      val reads = iterators(0)
      function(locus, loci, reads)
    })
  }

  /**
   * Same as [[windowTaskFlatMap]], except works with two RDDs of reads.
   *
   * The function will be passed two iterators of reads, one for each RDD.
   *
   * See [[windowTaskFlatMap]] for more details.
   */

  def windowTaskFlatMapTwoRDDs[T: ClassTag](
    reads1: RDD[MappedRead],
    reads2: RDD[MappedRead],
    lociPartitions: LociMap[Long],
    halfWindowSize: Long,
    function: (Long, LociSet, Iterator[MappedRead], Iterator[MappedRead]) => Iterator[T]): RDD[T] = {
    windowTaskFlatMapMultipleRDDs(Seq(reads1, reads2), lociPartitions, halfWindowSize, (locus, loci, readsPair) => {
      assert(readsPair.length == 2)
      val reads1 = readsPair(0)
      val reads2 = readsPair(1)
      function(locus, loci, reads1, reads2)
    })
  }

  /**
   * FlatMap across sets of reads overlapping genomic partitions, on multiple RDDs.
   *
   * Although this function would from its interface appear to support any number of RDDs, as a matter of implementation
   * we currently only support working with 1 or 2 read RDDs. That is, the readRDDs param must currently be length 1 or 2.
   *
   * This function works as follows:
   *
   *  (1) Assign reads to partitions. A read may overlap multiple partitions, and therefore be assigned to multiple
   *      partitions.
   *
   *  (2) For each partition, call the provided function. The arguments to this function are the task number, the loci
   *      assigned to this task, and a sequence of iterators giving the reads overlapping those loci (within the
   *      specified halfWindowSize) from each corresponding input RDD. The loci assigned to this task are always unique
   *      to this task, but the same reads may be provided to multiple tasks, since reads may overlap loci partition
   *      boundaries.
   *
   *  (3) The results of the provided function are concatenated into an RDD, which is returned.
   *
   * @param readsRDDs sequence of RDD[MappedRead].
   * @param lociPartitions map from locus -> task number. This argument specifies both the loci to be considered and how
   *                       they should be split among tasks. Reads that don't overlap these loci are discarded.
   * @param halfWindowSize if a read overlaps a region of halfWindowSize to either side of a locus under consideration,
   *                       then it is included.
   * @param function function to flatMap: (task number, loci, sequence of iterators of reads that overlap a window
   *                 around these loci) -> T
   * @tparam T type of value returned by function
   * @return flatMap results, RDD[T]
   */
  private def windowTaskFlatMapMultipleRDDs[T: ClassTag](
    readsRDDs: Seq[RDD[MappedRead]],
    lociPartitions: LociMap[Long],
    halfWindowSize: Long,
    function: (Long, LociSet, Seq[Iterator[MappedRead]]) => Iterator[T]): RDD[T] = {

    assume(readsRDDs.length > 0)
    val sc = readsRDDs(0).sparkContext
    progress("Loci partitioning: %s".format(lociPartitions.truncatedString()))
    val lociPartitionsBoxed: Broadcast[LociMap[Long]] = sc.broadcast(lociPartitions)
    val numTasks = lociPartitions.asInverseMap.map(_._1).max + 1

    // Counters
    val totalReads = sc.accumulator(0L)
    val relevantReads = sc.accumulator(0L)
    val expandedReads = sc.accumulator(0L)
    DelayedMessages.default.say { () =>
      "Read counts: filtered %,d total reads to %,d relevant reads, expanded for overlaps by %,.2f%% to %,d".format(
        totalReads.value,
        relevantReads.value,
        (expandedReads.value - relevantReads.value) * 100.0 / relevantReads.value,
        expandedReads.value)
    }

    // Expand reads into (task, read) pairs for each read RDD.
    val taskNumberReadPairsRDDs = readsRDDs.map(reads => reads.flatMap(read => {
      val singleContig = lociPartitionsBoxed.value.onContig(read.referenceContig)
      val thisReadsTasks = singleContig.getAll(read.start - halfWindowSize, read.end.get + halfWindowSize)

      // Update counters
      totalReads += 1
      if (thisReadsTasks.nonEmpty) relevantReads += 1
      expandedReads += thisReadsTasks.size

      // Return this read, duplicated for each task it is assigned to.
      thisReadsTasks.map(task => (task, read))
    }))

    // Run the task on each partition. Keep track of the number of reads assigned to each task in an accumulator, so
    // we can print out a summary of the skew.
    val readsByTask = sc.accumulator(MutableHashMap.empty[String, Long])(new HashMapAccumulatorParam)
    DelayedMessages.default.say { () =>
      {
        assert(readsByTask.value.size == numTasks)
        val stats = new math3.stat.descriptive.DescriptiveStatistics()
        readsByTask.value.valuesIterator.foreach(stats.addValue(_))
        "Reads per task: min=%,.0f 25%%=%,.0f median=%,.0f (mean=%,.0f) 75%%=%,.0f max=%,.0f. Max is %,.2f%% more than mean.".format(
          stats.getMin,
          stats.getPercentile(25),
          stats.getPercentile(50),
          stats.getMean,
          stats.getPercentile(75),
          stats.getMax,
          ((stats.getMax - stats.getMean) * 100.0) / stats.getMean)
      }
    }

    // Here, we special case for different numbers of RDDs.
    val results = taskNumberReadPairsRDDs match {

      // One RDD.
      case taskNumberReadPairs :: Nil => {
        // Each key (i.e. task) gets its own partition.
        val partitioned = taskNumberReadPairs.partitionBy(new PartitionByKey(numTasks.toInt))
        partitioned.mapPartitionsWithIndex((taskNum: Int, taskNumAndReads) => {
          val taskLoci = lociPartitionsBoxed.value.asInverseMap(taskNum.toLong)
          val taskReads = taskNumAndReads.map(pair => {
            assert(pair._1 == taskNum)
            pair._2
          })

          // We need to invoke the function on an iterator of sorted reads. For now, we just load the reads into memory,
          // sort them by start position, and use an iterator of this. This of course means we load all the reads into memory,
          // which obviates the advantages of using iterators everywhere else. A better solution would be to somehow have
          // the data already sorted on each partition. Note that sorting the whole RDD of reads is unnecessary, so we're
          // avoiding it -- we just need that the reads on each task are sorted, no need to merge them across tasks.
          val allReads = taskReads.toSeq.sortBy(read => (read.referenceContig, read.start))

          readsByTask.add(MutableHashMap(taskNum.toString -> allReads.length))
          function(taskNum, taskLoci, Seq(allReads.iterator))
        })
      }

      // Two RDDs.
      case taskNumberReadPairs1 :: taskNumberReadPairs2 :: Nil => {
        // Cogroup-based implementation.
        val partitioned = taskNumberReadPairs1.cogroup(taskNumberReadPairs2, new PartitionByKey(numTasks.toInt))
        partitioned.mapPartitionsWithIndex((taskNum: Int, taskNumAndReadsPairs) => {
          if (taskNumAndReadsPairs.isEmpty) {
            Iterator.empty
          } else {
            val taskLoci = lociPartitionsBoxed.value.asInverseMap(taskNum.toLong)
            val taskNumAndPair = taskNumAndReadsPairs.next()
            assert(taskNumAndReadsPairs.isEmpty)
            assert(taskNumAndPair._1 == taskNum)
            val taskReads1 = taskNumAndPair._2._1.toSeq.sortBy(read => (read.referenceContig, read.start))
            val taskReads2 = taskNumAndPair._2._2.toSeq.sortBy(read => (read.referenceContig, read.start))
            readsByTask.add(MutableHashMap(taskNum.toString -> (taskReads1.length + taskReads2.length)))
            val result = function(taskNum, taskLoci, Seq(taskReads1.iterator, taskReads2.iterator))
            result
          }
        })
      }

      // We currently do not support the general case.
      case _ => throw new AssertionError("Unsupported number of RDDs: %d".format(taskNumberReadPairsRDDs.length))
    }
    results
  }

  /**
   * Using an iterator of reads sorted by (contig, start locus), this class exposes a way to get separate iterators
   * over the reads in each contig.
   *
   * For example, given these reads (written as contig:start locus):
   *    chr20:1000,chr20:1500,chr21:200
   *
   * Calling next("chr20") will return an iterator of two reads (chr20:1000 and chr20:1500). After that, calling
   * next("chr21") will give an iterator of one read (chr21:200).
   *
   * Note that you must call next("chr20") before calling next("chr21") in this example. That is, this class does not
   * buffer anything -- it just walks forward in the reads using the iterator you gave it.
   *
   * Also note that after calling next("chr21"), the iterator returned by our previous call to next() is invalidated.
   *
   * @param readIterator reads, sorted by contig and start locus.
   */
  class ReadsByContig(readIterator: Iterator[MappedRead]) {
    private val buffered = readIterator.buffered
    private var seenContigs = List.empty[String]
    private var prevIterator: Option[SingleContigReadsIterator] = None
    def next(contig: String): Iterator[MappedRead] = {
      // We must first march the previous iterator we returned to the end.
      while (prevIterator.exists(_.hasNext)) prevIterator.get.next()

      // The next element from the iterator should have a contig we haven't seen so far.
      assert(buffered.isEmpty || !seenContigs.contains(buffered.head.referenceContig),
        "Reads are not sorted by contig. Contigs requested so far: %s. Next read's contig: %s.".format(
          seenContigs.reverse.toString, buffered.head.referenceContig))
      seenContigs ::= contig

      // Wrap our iterator and return it.
      prevIterator = Some(new SingleContigReadsIterator(contig, buffered))
      prevIterator.get
    }
  }

  /**
   * Wraps an iterator of reads sorted by contig name. Implements an iterator that gives reads only for the specified
   * contig name, then stops.
   */
  class SingleContigReadsIterator(contig: String, iterator: BufferedIterator[MappedRead]) extends Iterator[MappedRead] {
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
