package org.hammerlab.guacamole.loci.partitioning

import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.loci.LociArgs
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.reference.ReferenceRegion
import org.hammerlab.magic.args4j.StringOptionHandler
import org.kohsuke.args4j.{Option => Args4JOption}

import scala.reflect.ClassTag

trait LociPartitionerArgs
  extends LociArgs
  with ApproximatePartitionerArgs
  with UniformPartitionerArgs {

  @Args4JOption(
    name = "--loci-partitioning",
    usage = "Directory path within which to save the partitioned reads and accompanying LociMap partitioning.",
    forbids = Array("--partitioning-dir"),
    handler = classOf[StringOptionHandler]
  )
  var lociPartitioningPathOpt: Option[String] = None

  @Args4JOption(
    name = "--loci-partitioner",
    usage = "Loci partitioner to use: 'approximate' or 'uniform' (default: 'approximate')."
  )
  var lociPartitionerName: String = "approximate"

  def getPartitioner[R <: ReferenceRegion: ClassTag](regions: RDD[R], halfWindowSize: Int = 0): LociPartitioner = {
    val sc = regions.sparkContext
    val numPartitions =
      if (parallelism == 0)
        sc.defaultParallelism
      else
        parallelism

    lociPartitionerName match {
      case "approximate" =>
        new ApproximatePartitioner(regions, halfWindowSize, numPartitions, partitioningAccuracy)
      case "uniform" =>
        new UniformPartitioner(numPartitions)
      case _ =>
        throw new IllegalArgumentException(s"Unrecognized --loci-partitioner: $lociPartitionerName")
    }
  }
}

trait LociPartitioner {
  def partition(loci: LociSet): LociPartitioning
}

object LociPartitioner {
  // Convenience types representing Spark partition indices, or numbers of Spark partitions.
  type PartitionIndex = Int
  type NumPartitions = Int
}
