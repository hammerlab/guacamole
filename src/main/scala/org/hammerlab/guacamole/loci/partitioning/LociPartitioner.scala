package org.hammerlab.guacamole.loci.partitioning

import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.loci.LociArgs
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.reference.ReferenceRegion
import org.hammerlab.magic.args4j.StringOptionHandler
import org.kohsuke.args4j.spi.BooleanOptionHandler
import org.kohsuke.args4j.{Option => Args4JOption}

import scala.reflect.ClassTag

trait LociPartitionerArgs extends LociArgs

trait AllLociPartitionerArgs
  extends ApproximatePartitionerArgs
    with ExactPartitionerArgs {

  @Args4JOption(
    name = "--partitioning-dir",
    usage = "Directory from which to read an existing partition-reads RDD, with accompanying LociMap partitioning.",
    forbids = Array("--partitioned-reads-path", "--loci-partitioning-path"),
    handler = classOf[StringOptionHandler]
  )
  private var partitioningDirOpt: Option[String] = None

  @Args4JOption(
    name = "--partitioned-reads",
    usage = "Directory from which to read an existing partition-reads RDD, with accompanying LociMap partitioning.",
    forbids = Array("--partitioning-dir"),
    handler = classOf[StringOptionHandler]
  )
  private var _partitionedReadsPathOpt: Option[String] = None

  def partitionedReadsPathOpt: Option[String] =
    _partitionedReadsPathOpt
      .orElse(
        partitioningDirOpt
          .map(
            new Path(_, "reads").toString
          )
      )

  @Args4JOption(
    name = "--loci-partitioning",
    usage = "Directory path within which to save the partitioned reads and accompanying LociMap partitioning.",
    forbids = Array("--partitioning-dir"),
    handler = classOf[StringOptionHandler]
  )
  private var _lociPartitioningPathOpt: Option[String] = None

  def lociPartitioningPathOpt: Option[String] =
    _lociPartitioningPathOpt
      .orElse(
        partitioningDirOpt
        .map(
          new Path(_, "partitioning").toString
        )
      )

  @Args4JOption(
    name = "--loci-partitioner",
    usage = "Loci partitioner to use: 'exact', 'approximate', or 'uniform' (default: 'exact')."
  )
  var lociPartitionerName: String = "exact"

  def getPartitioner[R <: ReferenceRegion: ClassTag](regions: RDD[R], halfWindowSize: Int = 0): LociPartitioner = {
    if (lociPartitionerName == "exact")
      new ExactPartitioner(regions, halfWindowSize, maxReadsPerPartition, printStats = !quiet): LociPartitioner
    else if (lociPartitionerName == "approximate")
      new ApproximatePartitioner(regions, halfWindowSize, parallelism, partitioningAccuracy): LociPartitioner
    else if (lociPartitionerName == "uniform")
      new UniformPartitioner(parallelism): LociPartitioner
    else
      throw new IllegalArgumentException(s"Unrecognized --loci-partitioner: $lociPartitionerName")
  }

  @Args4JOption(
    name = "--compress",
    usage = "Whether to compress the output partitioned reads (default: true).",
    handler = classOf[BooleanOptionHandler]
  )
  var compressReadPartitions: Boolean = false

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
