package org.hammerlab.guacamole.readsets.rdd

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.reads.{MappedRead, ReadsUtil}
import org.hammerlab.guacamole.readsets.ReadSets
import org.hammerlab.guacamole.readsets.args.SingleSampleArgs
import org.hammerlab.guacamole.readsets.io.{InputFilters, ReadLoadingConfig}
import org.hammerlab.guacamole.util.TestUtil

trait ReadsRDDUtil
  extends ReadsUtil {

  def sc: SparkContext

  def makeReadsRDD(reads: (String, String, Int)*): RDD[MappedRead] =
    sc.parallelize(
      for {
        (sequence, cigar, start) <- reads
      } yield
        makeRead(sequence, cigar, start)
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
                   filters: InputFilters = InputFilters.empty,
                   config: ReadLoadingConfig = ReadLoadingConfig.default): ReadsRDD = {
    // Grab the path to the SAM file in the resources subdirectory.
    val path = TestUtil.testDataPath(filename)
    assert(sc != null)
    assert(sc.hadoopConfiguration != null)
    val args = new SingleSampleArgs {}
    args.reads = path
    ReadSets.loadReads(args, sc, filters)._1
  }
}
