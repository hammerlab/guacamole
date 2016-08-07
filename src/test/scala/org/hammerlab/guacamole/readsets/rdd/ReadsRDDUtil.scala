package org.hammerlab.guacamole.readsets.rdd

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.jointcaller.InputCollection
import org.hammerlab.guacamole.loci.set.LociParser
import org.hammerlab.guacamole.reads.{MappedRead, ReadsUtil, TestRegion}
import org.hammerlab.guacamole.readsets.ReadSets
import org.hammerlab.guacamole.readsets.io.InputFilters
import org.hammerlab.guacamole.util.TestUtil

trait ReadsRDDUtil extends ReadsUtil {

  def sc: SparkContext

  def makeReadsRDD(reads: (String, String, Int)*): RDD[MappedRead] =
    sc.parallelize(
      for {
        (sequence, cigar, start) <- reads
      } yield
        TestUtil.makeRead(sequence, cigar, start)
    )

  def makeRegionsRDD(numPartitions: Int, reads: (String, Int, Int, Int)*): RDD[TestRegion] =
    sc.parallelize(makeReads(reads).toSeq, numPartitions)

  def makeReadSets(inputs: InputCollection, loci: LociParser): ReadSets =
    ReadSets(sc, inputs.items, filters = InputFilters(overlapsLoci = loci))
}
