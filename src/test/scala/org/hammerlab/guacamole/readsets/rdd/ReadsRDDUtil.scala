package org.hammerlab.guacamole.readsets.rdd

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.reads.{MappedRead, ReadsUtil, TestRegion}
import org.hammerlab.guacamole.util.TestUtil

trait ReadsRDDUtil extends ReadsUtil {

  def sc: SparkContext

  def makeReadsRDD(reads: (String, String, Int)*): RDD[MappedRead] =
    sc.parallelize(
      for {
        (seq, cigar, start) <- reads
      } yield
        TestUtil.makeRead(seq, cigar, start)
    )
}
