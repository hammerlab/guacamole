package org.hammerlab.guacamole.loci.partitioning

import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.loci.LociArgs
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.reference.ReferenceRegion
import org.hammerlab.magic.args4j.StringOptionHandler
import org.kohsuke.args4j.spi.BooleanOptionHandler
import org.kohsuke.args4j.{Option => Args4JOption}

import scala.reflect.ClassTag

trait LociPartitionerArgs
  extends LociArgs
    with MicroRegionPartitionerArgs
    with CappedRegionsPartitionerArgs
    with UniformPartitionerArgs {

  @Args4JOption(
    name = "--loci-partitioning",
    usage = "If set: load a LociPartitioning from this path if it exists; write a computed partitioning to this path if it doesn't.",
    handler = classOf[StringOptionHandler]
  )
  var lociPartitioningPathOpt: Option[String] = None

  @Args4JOption(
    name = "--loci-partitioner",
    usage = "Loci partitioner to use: 'exact', 'approximate', or 'uniform' (default: 'exact')."
  )
  var lociPartitionerName: String = "exact"

  def getPartitioner[R <: ReferenceRegion: ClassTag](regions: RDD[R], halfWindowSize: Int = 0): LociPartitioner = {
    val sc = regions.sparkContext
    val numPartitions =
      if (parallelism == 0)
        sc.defaultParallelism
      else
        parallelism

    lociPartitionerName match {
      case "exact" =>
        new CappedRegionsPartitioner(regions, halfWindowSize, maxReadsPerPartition, printStats = !quiet)
      case "approximate" =>
        new MicroRegionPartitioner(regions, halfWindowSize, numPartitions, partitioningAccuracy)
      case "uniform" =>
        UniformPartitioner(numPartitions)
      case _ =>
        throw new IllegalArgumentException(s"Unrecognized --loci-partitioner: $lociPartitionerName")
    }
  }

  @Args4JOption(
    name = "--quiet",
    aliases = Array("-q"),
    usage = "Whether to compute additional statistics about the partitioned reads (default: false).",
    handler = classOf[BooleanOptionHandler]
  )
  var quiet: Boolean = false
}

trait LociPartitioner {
  def partition(loci: LociSet): LociPartitioning
}

object LociPartitioner {
  // Convenience types representing Spark partition indices, or numbers of Spark partitions.
  type PartitionIndex = Int
  type NumPartitions = Int
}
