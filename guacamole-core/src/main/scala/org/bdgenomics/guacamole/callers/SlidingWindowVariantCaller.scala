package org.bdgenomics.guacamole.callers

import org.bdgenomics.guacamole.{ Common, LociSet, SlidingReadWindow }
import org.bdgenomics.guacamole.Common.progress
import org.bdgenomics.adam.rich.RichADAMRecord._
import org.bdgenomics.adam.avro.{ ADAMRecord, ADAMGenotype }
import org.bdgenomics.adam.rdd._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._
import org.kohsuke.args4j.{ Option => Opt }
import scala.Option
import org.bdgenomics.guacamole.Common.Arguments.{ Loci, Base }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rich.RichADAMRecord

trait SlidingWindowVariantCaller {
  /**
   * The size of the sliding window (number of bases to either side of a locus) requested by this variant caller
   * implementation.
   *
   * Implementations must override this.
   *
   */
  val halfWindowSize: Long

  /**
   * Given a the samples to call variants for, a [[SlidingReadWindow]], and loci on one contig to call variants at, returns an iterator of
   * genotypes giving the result of variant calling. The [[SlidingReadWindow]] will have the window size requested
   * by this variant caller implementation.
   *
   * Implementations must override this.
   *
   */
  def callVariants(samples: Seq[String], reads: SlidingReadWindow, loci: LociSet.SingleContig): Iterator[ADAMGenotype]
}
object SlidingWindowVariantCaller {
  trait Arguments extends Base with Loci {
    @Opt(name = "-no-sort", usage = "Don't sort reads: use if reads ar already stored sorted.")
    var noSort: Boolean = false

    @Opt(name = "-parallelism", usage = "Num variant calling tasks. Set to 0 (default) to call variants on the Spark master")
    var parallelism: Int = 0
  }

  /**
   * Call variants using reads stored in a Spark RDD.
   *
   *
   * @param reads Spark RDD of ADAM reads. May be in any order if sorting is enabled
   * @param caller Variant calling implementation to use.
   * @param loci Loci to call variants at.
   * @param parallelism Number of spark workers to use to call variants. Set to 0 to stream reads to the spark master
   *                    and run the variant calling locally.
   * @return An RDD of the called variants.
   */

  /**
   * Call variants using the given SlidingWindowVariantCaller.
   *
   * @param args parsed commandline arguments
   * @param caller a variant caller instance
   * @param reads reads to use to call variants
   * @return the variants
   */
  def invoke(args: Arguments, caller: SlidingWindowVariantCaller, reads: RDD[ADAMRecord]): RDD[ADAMGenotype] = {
    val loci = Common.loci(args, reads)

    val includedReads = reads.filter(overlaps(_, loci, caller.halfWindowSize))
    progress("Filtered to %d reads that overlap loci of interest: %s.".format(includedReads.count, loci.toString))

    val samples = reads.map(read => Option(read.recordGroupSample).map(_.toString).getOrElse("default")).distinct.collect
    progress("Reads contain %d sample(s).".format(samples.length))

    // Sort reads by start.
    val sorted = if (!args.noSort) includedReads.adamSortReadsByReferencePosition else includedReads

    if (args.parallelism == 0) {
      // Serial implementation on Spark master.
      val allReads = sorted.collect.iterator
      progress("Collected reads.")
      val readsSplitByContig = splitReadsByContig(allReads, loci.contigs)
      val slidingWindows = readsSplitByContig.mapValues(SlidingReadWindow(caller.halfWindowSize, _))

      // Reads are coming in sorted by contig, so we process one contig at a time, in order.
      val genotypes = slidingWindows.toSeq.sortBy(_._1).flatMap({
        case (contig, window) => caller.callVariants(samples, window, loci.onContig(contig))
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
      val (withContig, withoutContig) = currentIterator.partition(_.contig.contigName == contig)
      currentIterator = withoutContig
      (contig, withContig)
    }).toMap + ("" -> currentIterator)
  }

  /**
   * Does the given read overlap any of the given loci, with halfWindowSize padding?
   */
  private def overlaps(read: RichADAMRecord, loci: LociSet, halfWindowSize: Long = 0): Boolean = {
    read.getReadMapped && loci.onContig(read.contig.contigName.toString).intersects(
      math.min(0, read.getStart - halfWindowSize),
      read.end.get + halfWindowSize)
  }

}
