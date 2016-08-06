package org.hammerlab.guacamole.readsets.rdd

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.jointcaller.InputCollection
import org.hammerlab.guacamole.loci.set.LociParser
import org.hammerlab.guacamole.reads.{MappedRead, ReadsUtil}
import org.hammerlab.guacamole.readsets.io.InputFilters
import org.hammerlab.guacamole.readsets.{ReadSets, SampleId}

trait ReadsRDDUtil extends ReadsUtil {

  def sc: SparkContext

  def makeReadsRDD(reads: (String, String, Int)*): RDD[MappedRead] = makeReadsRDD(sampleId = 0, reads: _*)

  def makeReadsRDD(sampleId: SampleId, reads: (String, String, Int)*): RDD[MappedRead] =
    sc.parallelize(
      for {
        (sequence, cigar, start) <- reads
      } yield
        makeRead(sequence, cigar, start, sampleId = sampleId)
    )

  def makeReadSets(inputs: InputCollection, loci: LociParser): ReadSets =
    ReadSets(sc, inputs.items, filters = InputFilters(overlapsLoci = loci))
}
