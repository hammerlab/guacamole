package org.hammerlab.guacamole.readsets.rdd

import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.loci.partitioning.LociPartitioning
import org.hammerlab.guacamole.readsets.PerSample
import org.hammerlab.guacamole.reference.ReferenceRegion

import scala.reflect.ClassTag

trait PartitionedRegionsUtil {

  def partitionReads[R <: ReferenceRegion: ClassTag](readsRDDs: PerSample[RDD[R]],
                                                     lociPartitioning: LociPartitioning): PartitionedRegions[R] = {
    PartitionedRegions(
      readsRDDs,
      lociPartitioning,
      halfWindowSize = 0,
      partitionedRegionsPathOpt = None,
      compress = false,
      printStats = false
    )
  }
}
