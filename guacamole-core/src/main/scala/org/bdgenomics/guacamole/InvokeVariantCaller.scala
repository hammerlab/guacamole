package org.bdgenomics.guacamole

import org.bdgenomics.adam.avro.{ADAMGenotype, ADAMRecord}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.RangePartitioner
import org.bdgenomics.guacamole.callers.VariantCaller

object InvokeVariantCaller {
  
  def usingSpark(reads: RDD[ADAMRecord], caller: VariantCaller, numPartitions: Int = 100): Seq[ADAMGenotype] = {
    val mappedReads = reads.filter(_.getReadMapped)
    log.info("Filtered: %d reads total -> %d mapped reads".format(reads.count, mappedReads.count))

    val keyed = mappedReads.keyBy(read => (read.getReferenceName, read.getStart))
    val sorted = keyed.sortByKey()
    sorted.partitionBy(new RangePartitioner(100, sorted))
    sorted.m


    val sortedReads = if (sort) {
      .sortByKey().map(_._2)
    } else {
      mappedReads
    }

    mappedReads.

    mappedReads

    val genotypes = sortedReads.mapPartitions(sortedReadIterator => {
      val window = new SlidingReadWindow()

    }

    val window = SlidingReadWindow



    reads.
  }

}
