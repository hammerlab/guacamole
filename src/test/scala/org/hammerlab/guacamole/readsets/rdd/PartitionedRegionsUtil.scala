package org.hammerlab.guacamole.readsets.rdd

import org.apache.spark.rdd.RDD
import org.hammerlab.genomics.reference.Region
import org.hammerlab.guacamole.loci.partitioning.LociPartitioning

import scala.reflect.ClassTag

trait PartitionedRegionsUtil {

  def partitionReads[R <: Region: ClassTag](reads: RDD[R],
                                            lociPartitioning: LociPartitioning): PartitionedRegions[R] =
    PartitionedRegions(
      reads,
      lociPartitioning,
      halfWindowSize = 0,
      partitionedRegionsPathOpt = None,
      compress = false,
      printStats = false
    )
}
