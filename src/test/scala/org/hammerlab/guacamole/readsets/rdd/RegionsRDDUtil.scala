package org.hammerlab.guacamole.readsets.rdd

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.hammerlab.genomics.reference.test.LocusUtil
import org.hammerlab.genomics.reference.{ ContigName, Locus, Region }
import org.hammerlab.guacamole.reads.RegionsUtil

trait RegionsRDDUtil
  extends RegionsUtil
    with LocusUtil {

  def sc: SparkContext

  def makeRegionsRDD(numPartitions: Int, reads: (ContigName, Locus, Locus, Int)*): RDD[Region] =
    sc.parallelize(makeRegions(reads: _*), numPartitions)
}
