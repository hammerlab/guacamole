package org.hammerlab.guacamole.commands

import org.apache.spark.SparkContext
import org.hammerlab.guacamole.loci.partitioning.HalfWindowArgs
import org.hammerlab.guacamole.readsets.ReadSets
import org.hammerlab.guacamole.readsets.args.{Arguments => ReadSetsArgs}
import org.hammerlab.guacamole.readsets.rdd.{PartitionedRegions, PartitionedRegionsArgs}

class PartitionReadsArgs
  extends Args
    with ReadSetsArgs
    with PartitionedRegionsArgs
    with HalfWindowArgs

object PartitionReads extends SparkCommand[PartitionReadsArgs] {
  override val name: String = "partition-reads"
  override val description: String = "Partition some reads and output statistics about them."

  override def run(args: PartitionReadsArgs, sc: SparkContext): Unit = {
    val (readsets, loci) = ReadSets(sc, args)
    PartitionedRegions(readsets.allMappedReads, loci, args)
  }
}
