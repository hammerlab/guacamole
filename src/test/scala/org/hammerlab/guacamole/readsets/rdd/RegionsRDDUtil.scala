package org.hammerlab.guacamole.readsets.rdd

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.reads.TestRegion
import org.hammerlab.guacamole.reference.RegionsUtil

trait RegionsRDDUtil
  extends RegionsUtil {

  def sc: SparkContext

  def makeRegionsRDD(numPartitions: Int, reads: (String, Int, Int, Int)*): RDD[TestRegion] =
    sc.parallelize(makeRegions(reads).toSeq, numPartitions)
}
