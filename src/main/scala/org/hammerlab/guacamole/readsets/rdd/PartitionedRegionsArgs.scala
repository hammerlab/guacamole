package org.hammerlab.guacamole.readsets.rdd

import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.loci.partitioning.{LociPartitionerArgs, LociPartitioning}
import org.hammerlab.magic.args4j.StringOptionHandler
import org.kohsuke.args4j.spi.BooleanOptionHandler
import org.kohsuke.args4j.{Option => Args4JOption}

/**
 * Command-line arguments related to partitioning regions according to a [[LociPartitioning]].
 *
 * Allows for configuring how/whether to persist the computed [[PartitionedRegions]] (its [[RDD]] of regions and/or its
 * [[LociPartitioning]]).
 *
 * The `--partitioning-dir`, if present, specifies a location that both read- and loci- partitionings will be saved to;
 * each one can also be set individually via `--partitioning-dir` and `--loci-partitioning`.
 */
trait PartitionedRegionsArgs extends LociPartitionerArgs {
  @Args4JOption(
    name = "--partitioning-dir",
    usage =
      "Directory from which to read an existing partition-reads RDD (and accompanying LociPartitioning), if the " +
        "directory exists; otherwise, save them here. If set, precludes use of --partitioned-reads and " +
        "--loci-partitioning",
    forbids = Array("--partitioned-reads-path", "--loci-partitioning-path"),
    handler = classOf[StringOptionHandler]
  )
  private var partitioningDirOpt: Option[String] = None

  @Args4JOption(
    name = "--partitioned-reads",
    usage = "Directory from which to read an existing partition-reads RDD, if it exists; otherwise, save it here.",
    forbids = Array("--partitioning-dir"),
    handler = classOf[StringOptionHandler]
  )
  private var _partitionedReadsPathOpt: Option[String] = None

  // Default to `partitioningDirOpt` if it is set.
  def partitionedReadsPathOpt: Option[String] =
    _partitionedReadsPathOpt
    .orElse(
      partitioningDirOpt
      .map(
        new Path(_, "reads").toString
      )
    )

  // Override the loci-partitioning-path lookup to default to `partitioningDirOpt` if the latter is set.
  override def lociPartitioningPathOpt: Option[String] =
    _lociPartitioningPathOpt
      .orElse(
        partitioningDirOpt
        .map(
          new Path(_, "partitioning").toString
        )
      )

  @Args4JOption(
    name = "--compress",
    usage = "Whether to compress the output partitioned reads (default: false).",
    handler = classOf[BooleanOptionHandler]
  )
  var compressReadPartitions: Boolean = false
}
