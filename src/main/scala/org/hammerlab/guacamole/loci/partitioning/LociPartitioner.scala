package org.hammerlab.guacamole.loci.partitioning

import java.util.NoSuchElementException

import org.apache.spark.rdd.RDD
import org.hammerlab.args4s.StringOptionHandler
import org.hammerlab.genomics.loci.set.LociSet
import org.hammerlab.genomics.reference.Region
import org.hammerlab.guacamole.loci.partitioning.LociPartitionerType.LociPartitionerType
import org.hammerlab.guacamole.readsets.rdd.PartitionedRegionsArgs
import org.kohsuke.args4j.{ Option => Args4JOption }

import scala.reflect.ClassTag

trait LociPartitionerArgs
  extends MicroRegionPartitionerArgs
    with CappedRegionsPartitionerArgs
    with UniformPartitionerArgs {

  @Args4JOption(
    name = "--loci-partitioning",
    usage = "Load a LociPartitioning from this path if it exists; else write a computed partitioning to this path.",
    handler = classOf[StringOptionHandler]
  )
  protected var _lociPartitioningPathOpt: Option[String] = None

  /**
   * Simple getter interface here supports overriding behavior here to support different resolution logic, cf.
   * [[PartitionedRegionsArgs]].
   */
  def lociPartitioningPathOpt: Option[String] = _lociPartitioningPathOpt

  @Args4JOption(
    name = "--loci-partitioner",
    usage = "Loci partitioner to use: 'capped', 'micro-regions', or 'uniform' (default: 'capped')."
  )
  protected var lociPartitionerName: String = "capped"

  private def lociPartitionerType: LociPartitionerType =
    try {
      LociPartitionerType.withName(lociPartitionerName)
    } catch {
      case _: NoSuchElementException =>
        throw new IllegalArgumentException(s"Unrecognized --loci-partitioner: $lociPartitionerName")
    }

  import LociPartitionerType._

  private[partitioning] def getPartitioner[R <: Region: ClassTag](regions: RDD[R]): LociPartitioner = {
    val sc = regions.sparkContext

    lociPartitionerType match {
      case Capped =>
        new CappedRegionsPartitioner(
          regions,
          halfWindowSize,
          maxReadsPerPartition,
          printPartitioningStats = printPartitioningStats,
          explodeCoverage = explodeCoverage,
          trimRanges = trimRanges
        )

      case Micro =>
        new MicroRegionPartitioner(regions, numPartitions(sc), partitioningAccuracy)

      case Uniform =>
        UniformPartitioner(numPartitions(sc))
    }
  }

  @Args4JOption(
    name = "--partitioning-stats",
    usage =
      "Compute additional statistics about the partitioned loci and reads; causes additional Spark jobs to be run, " +
        "so should be disabled in performance-critical environments. Default: false."
  )
  var printPartitioningStats: Boolean = false
}

object LociPartitionerType extends Enumeration {
  type LociPartitionerType = Value
  val Capped = Value("capped")
  val Micro = Value("micro-regions")
  val Uniform = Value("uniform")
}

trait LociPartitioner {
  def partition(loci: LociSet): LociPartitioning
}
