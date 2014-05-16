package org.bdgenomics.guacamole

import org.bdgenomics.guacamole.Common._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._
import org.bdgenomics.adam.avro.ADAMRecord
import org.apache.spark.broadcast.Broadcast
import org.bdgenomics.adam.rich.RichADAMRecord
import org.apache.spark.{ Accumulable, Partitioner, Logging }
import scala.reflect.ClassTag
import org.bdgenomics.guacamole.Common.Arguments.{ Loci, Base }
import org.kohsuke.args4j.{ Option => Opt }
import org.bdgenomics.guacamole.pileup.Pileup

object DistributedUtil extends Logging {
  trait Arguments extends Base with Loci {
    @Opt(name = "-parallelism", usage = "Num variant calling tasks. Set to 0 (default) to use the number of Spark partitions.")
    var parallelism: Int = 0

    @Opt(name = "-partition-accuracy",
      usage = "Num micro partitions to use per task in loci partitioning. Set to 0 to partition loci uniformly. Default: 1000.")
    var partitioningAccuracy: Int = 250
  }

  /**
   * Partition a LociSet among tasks according to the strategy specified in args.
   */
  def partitionLociAccordingToArgs(args: Arguments, reads: RDD[ADAMRecord], loci: LociSet): LociMap[Long] = {
    val tasks = if (args.parallelism > 0) args.parallelism else reads.partitions.length
    if (args.partitioningAccuracy == 0)
      partitionLociUniformly(tasks, loci)
    else
      partitionLociByApproximateReadDepth(reads, tasks, loci, args.partitioningAccuracy)
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
    for (contig <- loci.contigs; range <- loci.onContig(contig).ranges) {
      var start = range.start
      val end = range.end
      while (start < end) {
        val length: Long = math.min(remainingForThisTask, end - start)
        builder.put(contig, start, start + length, task)
        start += length
        lociAssigned += length
        if (remainingForThisTask == 0) task += 1
      }
    }
    val result = builder.result
    assert(lociAssigned == loci.count)
    assert(result.count == loci.count)
    result
  }

  /**
   * Assign loci from a LociSet to partitions, where each partition overlaps about the same number of reads.
   *
   * There are many ways we could do this. The approach we take here is:
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
   * @param reads RDD of reads
   * @param tasks number of partitions
   * @param loci loci to partition
   * @param accuracy integer >= 1. Higher values of this will result in a more exact but also more expensive computation.
   *                 Specifically, this is the number of micro partitions to use per task to estimate the read depth.
   *                 In the extreme case, setting this to greater than the number of loci per task will result in an
   *                 exact calculation.
   * @return LociMap of locus -> task assignments.
   */
  def partitionLociByApproximateReadDepth(reads: RDD[ADAMRecord], tasks: Int, loci: LociSet, accuracy: Int = 250): LociMap[Long] = {
    // Step (1). Split loci uniformly into micro partitions.
    assume(tasks >= 1)
    assume(loci.count > 0)
    val numMicroPartitions: Int = if (accuracy * tasks < loci.count) accuracy * tasks else loci.count.toInt
    progress("Splitting loci by read depth among %,d tasks using %,d micro partitions.".format(tasks, numMicroPartitions))
    val microPartitions = partitionLociUniformly(numMicroPartitions, loci)
    progress("Done calculating micro partitions.")
    val broadcastMicroPartitions = reads.sparkContext.broadcast(microPartitions)

    // Step (2)
    // Total up reads overlapping each micro partition. We keep the totals as an array of Longs.
    progress("Collecting read counts.")
    val counts = reads.mapPartitions(readIterator => {
      val microPartitions = broadcastMicroPartitions.value
      assert(microPartitions.count > 0)
      val counts = new Array[Long](numMicroPartitions)
      for (read <- readIterator) {
        val contigMap = microPartitions.onContig(read.getContig.getContigName.toString)
        val indices: Set[Long] = contigMap.getAll(read.start, RichADAMRecord(read).end.get)
        for (index: Long <- indices) {
          counts(index.toInt) += 1
        }
      }
      Seq(counts).iterator
    }).reduce((chunk1: Array[Long], chunk2: Array[Long]) => {
      assert(chunk1.length == chunk2.length)
      val result = new Array[Long](chunk1.length)
      for (i <- 0 until chunk1.length) {
        result(i) = chunk1(i) + chunk2(i)
      }
      result
    })

    // Step (3)
    // Assign loci to tasks, taking into account read depth in each micro partition.
    val totalReads = counts.sum
    val readsPerTask = math.max(1, totalReads.toDouble / tasks.toDouble)
    progress("Done collecting read counts. Total reads with micro partition overlaps: %,d = ~%,.0f reads per task."
      .format(totalReads, readsPerTask))
    progress("Reads per micro partition: min=%,d mean=%,.0f max=%,d.".format(
      counts.min, counts.sum.toDouble / counts.length, counts.max))
    val builder = LociMap.newBuilder[Long]
    var readsAssigned = 0.0
    var task = 0L
    def readsRemainingForThisTask = math.round(((task + 1) * readsPerTask) - readsAssigned).toLong
    for (microTask <- 0 until numMicroPartitions) {
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
          // We always take at least 1 locus to ensure we continue ot make progress.
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
    }
    val result = builder.result
    assert(result.count == loci.count)
    result
  }

  /**
   * Flatmap across loci, where at each locus the provided function is passed a Pileup instance.
   *
   * See [[windowTaskFlatMap()]] for argument descriptions.
   *
   */
  def pileupFlatMap[T: ClassTag](reads: RDD[ADAMRecord],
                                 lociPartitions: LociMap[Long],
                                 function: Pileup => Iterator[T]): RDD[T] = {
    windowTaskFlatMap(reads, lociPartitions, 0L, (task, taskLoci, taskReads) => {
      // The code here is running in parallel in each task.

      // A map from contig to iterator over reads that are in that contig:
      val readsSplitByContig = splitReadsByContig(taskReads, taskLoci.contigs)

      // Our result is the flatmap over both contigs and loci of the user function.
      readsSplitByContig.toSeq.sortBy(_._1).iterator.flatMap({
        case (contig, reads) => {
          val window = SlidingReadWindow(0L, reads)
          var maybePileup: Option[Pileup] = None
          taskLoci.onContig(contig).individually.flatMap(locus => {
            val newReads = window.setCurrentLocus(locus)
            maybePileup = Some(maybePileup match {
              case None         => Pileup(newReads, locus)
              case Some(pileup) => pileup.atGreaterLocus(locus, newReads.iterator)
            })
            function(maybePileup.get)
          })
        }
      })
    })
  }

  /**
   * FlatMap across loci, where at each locus the provided function is passed a SlidingWindow instance containing reads
   * overlapping that locus with the specified halfWindowSize.
   *
   * Currently unused, but here for demonstration.
   *
   * See [[windowTaskFlatMap()]] for argument descriptions.
   *
   */
  def windowFlatMap[T: ClassTag](reads: RDD[ADAMRecord],
                                 lociPartitions: LociMap[Long],
                                 halfWindowSize: Long,
                                 function: SlidingReadWindow => Iterator[T]): RDD[T] = {
    windowTaskFlatMap(reads, lociPartitions, halfWindowSize, (task, taskLoci, taskReads) => {
      val readsSplitByContig = splitReadsByContig(taskReads, taskLoci.contigs)
      val slidingWindows = readsSplitByContig.mapValues(SlidingReadWindow(halfWindowSize, _))
      slidingWindows.toSeq.sortBy(_._1).iterator.flatMap({
        case (contig, window) => {
          lociPartitions.onContig(contig).lociIndividually.flatMap(locus => {
            window.setCurrentLocus(locus)
            function(window)
          })
        }
      })
    })
  }

  /**
   * Spark partitioner for keyed RDDs that assigns each unique key its own partition.
   * Used to partition an RDD of (task number: Long, read: ADAMRecord) pairs, giving each task its own partition.
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
   * This function works as follows:
   *
   *  - Assign reads to partitions. A read may overlap multiple partitions, and therefore be assigned to multiple
   *    partitions.
   *
   *  - For each partition, call the provided function. The arguments to this function are the task number, the loci
   *    assigned to this task, and the reads overlapping those loci (within the specified halfWindowSize). The loci
   *    assigned to this task are always unique to this task, but the same reads may be provided to multiple tasks,
   *    since reads may overlap loci partition boundaries.
   *
   *  - The results of the provided function are concatenated into an RDD, which is returned.
   *
   * @param reads sorted mapped reads
   * @param lociPartitions map from locus -> task number. This argument specifies both the loci to be considered and how
   *                       they should be split among tasks. Reads that don't overlap these loci are discarded.
   * @param halfWindowSize if a read overlaps a region of halfWindowSize to either side of a locus under consideration,
   *                       then it is included.
   * @param function function to flatMap: (task number, loci, reads that overlap a window around these loci) -> T
   * @tparam T type of value returned by function
   * @return flatMap results, RDD[T]
   */
  def windowTaskFlatMap[T: ClassTag](reads: RDD[ADAMRecord],
                                     lociPartitions: LociMap[Long],
                                     halfWindowSize: Long,
                                     function: (Long, LociSet, Iterator[ADAMRecord]) => Iterator[T]): RDD[T] = {
    progress("Loci partitioning: %s".format(lociPartitions.truncatedString()))
    val lociPartitionsBoxed: Broadcast[LociMap[Long]] = reads.sparkContext.broadcast(lociPartitions)
    val numTasks = lociPartitions.asInverseMap.map(_._1).max + 1

    // Counters
    val totalReads = reads.sparkContext.accumulator(0L)
    val relevantReads = reads.sparkContext.accumulator(0L)
    val expandedReads = reads.sparkContext.accumulator(0L)
    DelayedMessages.default.say { () =>
      "Read counts: filtered %,d total reads to %,d relevant reads, expanded for overlaps by %,.2f%% to %,d".format(
        totalReads.value,
        relevantReads.value,
        (expandedReads.value - relevantReads.value) * 100.0 / relevantReads.value,
        expandedReads.value)
    }

    // Expand reads into (task, read) pairs.
    val taskNumberReadPairs = reads.map(RichADAMRecord(_)).flatMap(read => {
      val singleContig = lociPartitionsBoxed.value.onContig(read.getContig.getContigName.toString)
      val thisReadsTasks = singleContig.getAll(read.start - halfWindowSize, read.end.get + halfWindowSize)

      // Update counters
      totalReads += 1
      if (thisReadsTasks.nonEmpty) relevantReads += 1
      expandedReads += thisReadsTasks.size

      // Return this read, duplicated for each task it is assigned to.
      thisReadsTasks.map(task => (task, read.record))
    })

    // Each key (i.e. task) gets its own partition.
    val partitioned = taskNumberReadPairs.partitionBy(new PartitionByKey(numTasks.toInt))

    // Run the task on each partition.
    val results = partitioned.mapPartitionsWithIndex((taskNum: Int, taskNumAndReads) => {
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
      val taskReadsSeq = taskReads.toSeq.sortBy(_.start)
      function(taskNum, taskLoci, taskReadsSeq.iterator)
    })
    results
  }

  /**
   * Given an iterator of reads and the contig names found in these reads, return a Map where each contig name maps to
   * an iterator of reads that align to that contig. The map will additionally have an empty string element ("") that
   * maps to an iterator over reads that did not map to any contig specified.
   *
   * We are going out of our way here to use iterators everywhere so we don't force loading all the reads in at once.
   * Whether we in fact load in all the reads at once, however, depends on how the result of this function is used.
   * For example, if reads for some contig are found only at the end of the reads iterator, then advancing through the
   * iterator for that contig first will actually load all the reads into memory. Callers should pay attention to the sort
   * order of the reads if they want to avoid this.
   *
   */
  def splitReadsByContig(readIterator: Iterator[ADAMRecord], contigs: Seq[String]): Map[String, Iterator[ADAMRecord]] = {
    var currentIterator: Iterator[ADAMRecord] = readIterator
    contigs.map(contig => {
      val (withContig, withoutContig) = currentIterator.partition(_.contig.contigName.toString == contig)
      currentIterator = withoutContig
      (contig, withContig)
    }).toMap + ("" -> currentIterator)
  }
}
