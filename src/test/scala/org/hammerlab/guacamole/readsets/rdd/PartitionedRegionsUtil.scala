package org.hammerlab.guacamole.readsets.rdd

import org.apache.spark.rdd.RDD
import org.hammerlab.genomics.reference.Region
import org.hammerlab.guacamole.loci.partitioning.LociPartitioning
import org.hammerlab.guacamole.readsets.PartitionedReads

import scala.reflect.ClassTag

trait PartitionedRegionsUtil {

  def partitionReads[T: ClassTag, R <: Region: ClassTag](reads: RDD[T],
                                                         lociPartitioning: LociPartitioning)(implicit toR: T ⇒ R): PartitionedRegions[T, R] =
    PartitionedRegions(
      reads,
      lociPartitioning,
      halfWindowSize = 0,
      partitionedRegionsPathOpt = None,
      compress = false,
      printStats = false
    )
}
