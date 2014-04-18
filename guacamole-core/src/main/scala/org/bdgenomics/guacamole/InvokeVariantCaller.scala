package org.bdgenomics.guacamole

import org.bdgenomics.adam.avro.{ ADAMGenotype, ADAMRecord }
import org.apache.spark.rdd.RDD
import org.apache.spark.Logging
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.bdgenomics.guacamole.callers.VariantCaller
import org.bdgenomics.adam.rich.RichADAMRecord._
import org.bdgenomics.guacamole.Util.progress

/**
 * Functions to invoke variant calling.
 */
object InvokeVariantCaller extends Logging {
  /**
   * Call variants using reads stored in a Spark RDD.
   *
   * @param reads Spark RDD of ADAM reads. May be in any order.
   * @param caller Variant calling implementation to use.
   * @param loci Loci to call variants at.
   * @param parallelism Number of spark workers to use to call variants. Set to 0 to stream reads to the spark master
   *                    and run the variant calling locally.
   * @return An RDD of the called variants.
   */
  def usingSpark(reads: RDD[ADAMRecord], caller: VariantCaller, loci: LociSet, parallelism: Int = 100): RDD[ADAMGenotype] = {
    val includedReads = reads.filter(overlaps(_, loci, caller.halfWindowSize))
    progress("Filtered: %d reads total -> %d mapped and relevant reads".format(reads.count, includedReads.count))

    // Sort reads by start.
    val keyed = includedReads.keyBy(read => (read.getReferenceName.toString, read.getStart))
    val sorted = keyed.sortByKey(true)

    if (parallelism == 0) {
      // Serial implementation on Spark master.
      val allReads = sorted.map(_._2).collect.iterator
      progress("Collected reads.")
      val readsSplitByContig = splitReadsByContig(allReads, loci.contigs)
      val slidingWindows = readsSplitByContig.mapValues(SlidingReadWindow(caller.halfWindowSize, _))

      // Reads are coming in sorted by contig, so we process one contig at a time, in order.
      val genotypes = slidingWindows.toSeq.sortBy(_._1).flatMap({
        case (contig, window) => caller.callVariants(window, loci.onContig(contig))
      })
      reads.sparkContext.parallelize(genotypes)
    } else {
      throw new NotImplementedError("Distributed variant calling not yet supported.")
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
      val (withContig, withoutContig) = currentIterator.partition(_.getReferenceName == contig)
      currentIterator = withoutContig
      (contig, withContig)
    }).toMap + ("" -> currentIterator)
  }

  /**
   * Does the given read overlap any of the given loci, with halfWindowSize padding?
   */
  private def overlaps(read: ADAMRecord, loci: LociSet, halfWindowSize: Long = 0): Boolean = {
    read.getReadMapped && loci.onContig(read.getReferenceName.toString).intersects(
      math.min(0, read.getStart - halfWindowSize),
      read.end.get + halfWindowSize)
  }

}
