package org.hammerlab.guacamole.loci

import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.reads.MappedRead
import org.hammerlab.guacamole.reference.{ContigSequence, ReferenceBroadcast}

case class RegionStats(numReads: Int,
                       numReadsWithMismatches: Int,
                       numReadsWithIndels: Int) {

  def +(other: RegionStats): RegionStats = {
    RegionStats(
      numReads + other.numReads,
      numReadsWithMismatches + other.numReadsWithMismatches,
      numReadsWithIndels + other.numReadsWithIndels
    )
  }
}

object RegionStats {

  def apply(read: MappedRead, contigSequence: ContigSequence): RegionStats = {
    RegionStats(
      1,
      if (read.countOfMismatches(contigSequence) > 0) 1 else 0,
      if (read.cigar.numCigarElements() > 1) 1 else 0
    )
  }

}


class RegionStatsRDD(rdd: RDD[MappedRead],
                     reference: ReferenceBroadcast)  extends Serializable {

  @transient val sc = rdd.sparkContext
}
