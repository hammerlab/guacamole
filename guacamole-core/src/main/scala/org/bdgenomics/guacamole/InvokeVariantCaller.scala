package org.bdgenomics.guacamole

import org.bdgenomics.adam.avro.{ADAMGenotype, ADAMRecord}
import org.apache.spark.rdd.RDD
import org.apache.spark.Logging
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.bdgenomics.guacamole.callers.VariantCaller
import org.bdgenomics.adam.rich.RichADAMRecord._

object InvokeVariantCaller extends Logging {
  
  def usingSpark(reads: RDD[ADAMRecord], caller: VariantCaller, loci: LociRanges, parallelism: Int = 100): RDD[ADAMGenotype] = {
    val includedReads = reads.filter(overlaps(_, loci, caller.windowSize))
    log.info("Filtered: %d reads total -> %d mapped and relevant reads".format(reads.count, includedReads.count))

    // Sort reads by start.
    val keyed = includedReads.keyBy(read => (read.getReferenceName.toString, read.getStart))
    val sorted = keyed.sortByKey(true)

    if (parallelism == 0) {
      // Serial implementation on Spark master.
      val allReads = sorted.map(_._2).collect.iterator
      val readsSplitByContig = splitReadsByContig(allReads, loci.contigs)
      val slidingWindows = readsSplitByContig.mapValues(SlidingReadWindow(caller.windowSize, _))

      // Reads are coming in sorted by contig, so we process one contig at a time, in order.
      val genotypes = slidingWindows.toSeq.sortBy(_._1).flatMap({
        case (contig, window) => caller.callVariants(window, loci.at(contig))
      })
      reads.sparkContext.parallelize(genotypes)
    } else {
      throw new NotImplementedError("Distributed variant calling not yet supported.")
    }
  }

 private def splitReadsByContig(readIterator: Iterator[ADAMRecord], contigs: Seq[String]): Map[String, Iterator[ADAMRecord]] = {
   var currentIterator: Iterator[ADAMRecord] = readIterator
   contigs.map(contig => {
     val (withContig, withoutContig) = currentIterator.partition(_.getReferenceName == contig)
     currentIterator = withoutContig
     (contig, withContig)
   }).toMap + ("" -> currentIterator)
  }

  // Does a read overlap any of the loci we are calling variants at, with windowSize padding?
  def overlaps(read: ADAMRecord, loci: LociRanges, windowSize: Long = 0): Boolean = {
    read.getReadMapped && loci.intersects(
      read.getReferenceName.toString, math.min(0, read.getStart - windowSize), read.end.get + windowSize)
  }

}
