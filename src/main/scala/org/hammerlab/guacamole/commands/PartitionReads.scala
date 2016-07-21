package org.hammerlab.guacamole.commands

import org.apache.spark.SparkContext
import org.hammerlab.guacamole.loci.partitioning.AllLociPartitionerArgs
import org.hammerlab.guacamole.readsets.ReadSets
import org.hammerlab.guacamole.readsets.rdd.PartitionedRegions
import org.kohsuke.args4j.{Option => Args4jOption}

class PartitionReadsArgs extends ReadSets.Arguments with AllLociPartitionerArgs {
  @Args4jOption(
    name = "--half-window",
    usage = "Partitions get assigned all reads that overlap any base within this distance of either end of its range."
  )
  var halfWindow: Int = 50
}

/**
 * Load reads, partitions them, and optionally write the partitioning and partitioned reads to disk.
 */
object PartitionReads extends SparkCommand[PartitionReadsArgs] {
  override val name: String = "partition-reads"
  override val description: String = "Partition some reads and output statistics about them."

  override def run(args: PartitionReadsArgs, sc: SparkContext): Unit = {

    val readsets = ReadSets(sc, args.pathsAndSampleNames)
    val mappedReads = readsets.mappedReadsRDDs
    val ReadSets(_, sequenceDictionary, contigLengths) = readsets

    val loci = args.parseLoci(sc.hadoopConfiguration).result(contigLengths)

    PartitionedRegions(readsets.allMappedReads, loci, args, args.halfWindow)
  }
}
