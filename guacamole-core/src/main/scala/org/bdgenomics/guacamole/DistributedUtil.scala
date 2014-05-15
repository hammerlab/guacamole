package org.bdgenomics.guacamole

import org.bdgenomics.guacamole.Common._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._
import org.bdgenomics.adam.avro.ADAMRecord
import org.apache.spark.broadcast.Broadcast
import org.bdgenomics.adam.rich.RichADAMRecord
import org.apache.spark.{ Partitioner, Logging }
import scala.reflect.ClassTag
import org.bdgenomics.guacamole.Common.Arguments.{ Loci, Base }
import org.kohsuke.args4j.{ Option => Opt }
import org.bdgenomics.guacamole.pileup.Pileup

object DistributedUtil extends Logging {
  trait Arguments extends Base with Loci {
    @Opt(name = "-parallelism", usage = "Num variant calling tasks. Set to 0 (default) to use the number of Spark partitions.")
    var parallelism: Int = 0
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
   * Flatmap across loci, where at each locus the provided function is passed a Pileup instance.
   *
   * See [[windowTaskFlatMap()]] for argument descriptions.
   *
   */
  def pileupFlatMap[T: ClassTag](reads: RDD[ADAMRecord],
                                 loci: LociSet,
                                 tasks: Long,
                                 function: Pileup => Iterator[T]): RDD[T] = {
    windowTaskFlatMap(reads, loci, 0L, tasks, (task, taskLoci, taskReads) => {
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
                                 loci: LociSet,
                                 halfWindowSize: Long,
                                 tasks: Long,
                                 function: SlidingReadWindow => Iterator[T]): RDD[T] = {
    windowTaskFlatMap(reads, loci, halfWindowSize, tasks, (task, taskLoci, taskReads) => {
      val readsSplitByContig = splitReadsByContig(taskReads, taskLoci.contigs)
      val slidingWindows = readsSplitByContig.mapValues(SlidingReadWindow(halfWindowSize, _))
      slidingWindows.toSeq.sortBy(_._1).iterator.flatMap({
        case (contig, window) => {
          loci.onContig(contig).individually.flatMap(locus => {
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
   * Results for each partition can be computed in parallel. The degree of parallelism (i.e. the number of partitions) is
   * set by the tasks parameter.
   *
   * If tasks=0, then the result is computed on the spark master using one interval. For tasks > 0, the results are
   * computed on spark workers.
   *
   * This function works as follows:
   *
   *  - Partition the loci, num partitions = tasks.
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
   * @param loci loci to consider. Reads that don't overlap these loci are discarded.
   * @param halfWindowSize if a read overlaps a region of halfWindowSize to either side of a locus under consideration,
   *                       then it is included.
   * @param tasks number of loci partitions to run in parallel. Set to 0 to use number of partitions in the read RDD.
   * @param function function to flatMap: (task number, loci, reads that overlap a window around these loci) -> T
   * @tparam T type of value returned by function
   * @return flatMap results, RDD[T]
   */
  def windowTaskFlatMap[T: ClassTag](reads: RDD[ADAMRecord],
                                     loci: LociSet,
                                     halfWindowSize: Long,
                                     tasks: Long,
                                     function: (Long, LociSet, Iterator[ADAMRecord]) => Iterator[T]): RDD[T] = {
    val numTasks = if (tasks == 0) reads.partitions.length else tasks
    val taskMap = partitionLociUniformly(numTasks, loci)
    progress("Loci partitioning: %s".format(taskMap.truncatedString()))
    val taskMapBoxed: Broadcast[LociMap[Long]] = reads.sparkContext.broadcast(taskMap)

    // Counters)
    val totalReads = reads.sparkContext.accumulator(0L)
    val relevantReads = reads.sparkContext.accumulator(0L)
    val expandedReads = reads.sparkContext.accumulator(0L)
    DelayedMessages.default.say { () =>
      "Read counts: filtered %,d total reads to %,d relevant reads, expanded for overlaps to %,d".format(
        totalReads.value,
        relevantReads.value,
        expandedReads.value)
    }

    // Expand reads into (task, read) pairs.
    val taskNumberReadPairs = reads.map(RichADAMRecord(_)).flatMap(read => {
      val singleContig = taskMapBoxed.value.onContig(read.getContig.getContigName.toString)
      val thisReadsTasks = singleContig.getAll(read.start - halfWindowSize, read.end.get + halfWindowSize)

      // Update counters
      totalReads += 1
      if (thisReadsTasks.nonEmpty) relevantReads += 1
      expandedReads += thisReadsTasks.size

      // Return this read, duplicated for each task it is assigned to.1
      thisReadsTasks.map(task => (task, read.record))
    })

    // Each key (i.e. task) gets its own partition.
    val partitioned = taskNumberReadPairs.partitionBy(new PartitionByKey(numTasks.toInt))

    // Run the task on each partition.)
    val results = partitioned.mapPartitionsWithIndex((taskNum: Int, taskNumAndReads) => {
      val taskLoci = taskMapBoxed.value.asInverseMap(taskNum.toLong)
      val taskReads = taskNumAndReads.map(pair => {
        assert(pair._1 == taskNum)
        pair._2
      })
      function(taskNum, taskLoci, taskReads)
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
