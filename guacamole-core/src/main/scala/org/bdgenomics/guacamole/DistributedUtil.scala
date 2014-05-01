package org.bdgenomics.guacamole

import org.bdgenomics.guacamole.Common._
import org.bdgenomics.guacamole.SlidingReadWindow
import scala.collection.immutable.NumericRange
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._
import org.bdgenomics.adam.avro.ADAMRecord
import org.bdgenomics.adam.rich.RichADAMRecord
import org.apache.spark.{Logging, SparkContext}

object DistributedUtil extends Logging {

  def partitionLociUniformlyAmongTasks(tasks: Long, loci: LociSet): LociMap[Long] = {
    // TODO: read-aware partitioning.
    assume(tasks >= 1)
    val lociPerTask = loci.count.toDouble / tasks.toDouble
    progress("Splitting loci evenly among %d tasks = ~%.2f loci per task".format(tasks, lociPerTask))
    val builder = LociMap.newBuilder[Long]
    var lociAssigned = 0L
    loci.contigs.foreach(contig => {
      val queue = scala.collection.mutable.Stack[NumericRange[Long]]()
      queue.pushAll(loci.onContig(contig).ranges)
      while (!queue.isEmpty) {
        val range = queue.pop()
        val task = math.floor(lociAssigned / lociPerTask).toLong
        val remaining = math.round(((task + 1) * lociPerTask) - lociAssigned).toLong
        val length: Long = math.min(remaining, range.length)
        builder.put(contig, range.start, range.start + length, task)
        lociAssigned += length
        if (length < range.length) {
          queue.push(NumericRange[Long](range.start + length, range.end, 1))
        }
      }
    })
    builder.result
  }

  def slidingWindowFlatMap[T](reads: RDD[ADAMRecord], loci: LociSet, halfWindowSize: Long, tasks: Long, function: (LociSet, SlidingReadWindow) => Seq[T]): RDD[T] = {


  def slidingWindowMapPartitions[T](reads: RDD[ADAMRecord], loci: LociSet, halfWindowSize: Long, tasks: Long, function: (LociSet, SlidingReadWindow) => Seq[T]): RDD[T] = {
    if (tasks == 0) {
      progress("Collecting reads onto spark master.")
      val allReads = reads.collect.iterator
      progress("Done collecting reads.")
      assume(allReads.hasNext, "No reads")
      val readsSplitByContig = splitReadsByContig(allReads, loci.contigs)
      val slidingWindows = readsSplitByContig.mapValues(SlidingReadWindow(halfWindowSize, _))
      // Reads are coming in sorted by contig, so we process one contig at a time, in order.
      val results = slidingWindows.toSeq.sortBy(_._1).flatMap(pair => function(pair._2))
      reads.sparkContext.parallelize(results)
    } else {
      val taskMap = partitionLociUniformlyAmongTasks(tasks, loci)
      val tasksAndReads = reads.map(RichADAMRecord(_)).flatMap(read => {
        val singleContig = taskMap.onContig(read.getContig.getContigName.toString)
        val tasks = singleContig.getAll(read.start - halfWindowSize, read.end.get + halfWindowSize)
        tasks.map(task => (task, read.record))
      })
      progress("Expanded reads from %d to %d to handle overlaps between tasks".format(reads.count, tasksAndReads.count))
      val readsGroupedByTask = tasksAndReads.groupByKey(tasks.toInt)
      val results = readsGroupedByTask.flatMap({
        case (task, taskReads) => {
          val taskLoci = taskMap.asInverseMap(task)
          log.info("Task %d handling %d reads for loci: %s".format(task, taskReads.length, taskLoci))
          val readsSplitByContig = splitReadsByContig(taskReads.iterator, taskLoci.contigs)
          val slidingWindows = readsSplitByContig.mapValues(SlidingReadWindow(halfWindowSize, _))
          slidingWindows.toSeq.sortBy(_._1).flatMap(pair => function(pair._2))
        }
      })
      results
     }
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
  private def splitReadsByContig(readIterator: Iterator[ADAMRecord], contigs: Seq[String]): Map[String, Iterator[ADAMRecord]] = {
    var currentIterator: Iterator[ADAMRecord] = readIterator
    contigs.map(contig => {
      val (withContig, withoutContig) = currentIterator.partition(_.contig.contigName.toString == contig)
      currentIterator = withoutContig
      (contig, withContig)
    }).toMap + ("" -> currentIterator)
  }

  /**
   * Does the given read overlap any of the given loci, with halfWindowSize padding?
   */
  private def overlaps(read: RichADAMRecord, loci: LociSet, halfWindowSize: Long = 0): Boolean = {
    read.readMapped && loci.onContig(read.contig.contigName.toString).intersects(
      math.max(0, read.start - halfWindowSize),
      read.end.get + halfWindowSize)
  }



}
