package org.hammerlab.guacamole.readsets.rdd

import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.loci.partitioning.LociPartitioning
import org.hammerlab.guacamole.reference.ReferenceRegion

import scala.reflect.ClassTag

trait PartitionedRegionsUtil {
  def partitionReads[R <: ReferenceRegion: ClassTag](reads: RDD[R],
                                                     lociPartitioning: LociPartitioning): PartitionedRegions[R] = {
    PartitionedRegions(
      reads,
      lociPartitioning,
      halfWindowSize = 0,
      partitionedRegionsPathOpt = None,
      compress = false,
      printStats = false
    )
  }
}
