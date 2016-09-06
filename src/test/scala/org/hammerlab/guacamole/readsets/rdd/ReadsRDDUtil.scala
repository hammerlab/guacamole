package org.hammerlab.guacamole.readsets.rdd

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.reads.{MappedRead, ReadsUtil}
import org.hammerlab.guacamole.readsets.args.SingleSampleArgs
import org.hammerlab.guacamole.readsets.io.{InputFilters}
import org.hammerlab.guacamole.readsets.{ReadSets, SampleId}
import org.hammerlab.guacamole.util.TestUtil.resourcePath

trait ReadsRDDUtil
  extends ReadsUtil {

  def sc: SparkContext

  def makeReadsRDD(reads: (String, String, Int)*): RDD[MappedRead] = makeReadsRDD(sampleId = 0, reads: _*)

  def makeReadsRDD(sampleId: SampleId, reads: (String, String, Int)*): RDD[MappedRead] =
    sc.parallelize(
      for {
        (sequence, cigar, start) <- reads
      } yield
        makeRead(sequence, cigar, start, sampleId = sampleId)
    )

  def loadTumorNormalReads(sc: SparkContext,
                           tumorFile: String,
                           normalFile: String): (Seq[MappedRead], Seq[MappedRead]) = {
    val filters = InputFilters(mapped = true, nonDuplicate = true, passedVendorQualityChecks = true)
    (
      loadReadsRDD(sc, tumorFile, filters = filters).mappedReads.collect(),
      loadReadsRDD(sc, normalFile, filters = filters).mappedReads.collect()
    )
  }

  def loadReadsRDD(sc: SparkContext,
                   filename: String,
                   filters: InputFilters = InputFilters.empty): ReadsRDD = {
    // Grab the path to the SAM file in the resources subdirectory.
    val path = resourcePath(filename)
    assert(sc != null)
    assert(sc.hadoopConfiguration != null)
    val args = new SingleSampleArgs {}
    args.reads = path
    ReadSets.loadReads(args, sc, filters)._1
  }
}
