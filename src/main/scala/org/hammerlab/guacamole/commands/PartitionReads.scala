package org.hammerlab.guacamole.commands

import org.apache.spark.SparkContext
import org.hammerlab.commands.Args
import org.hammerlab.genomics.readsets.ReadSets
import org.hammerlab.genomics.readsets.args.impl.{ Arguments ⇒ ReadSetsArgs }
import org.hammerlab.guacamole.loci.partitioning.HalfWindowArgs
import org.hammerlab.guacamole.readsets.rdd.{ PartitionedRegions, PartitionedRegionsArgs }

class PartitionReadsArgs
  extends Args
    with ReadSetsArgs
    with PartitionedRegionsArgs
    with HalfWindowArgs

object PartitionReads extends GuacCommand[PartitionReadsArgs] {
  override val name: String = "partition-reads"
  override val description: String = "Partition some reads and output statistics about them."

  override def run(args: PartitionReadsArgs, sc: SparkContext): Unit = {
    val (readsets, loci) = ReadSets(sc, args)
    PartitionedRegions(readsets.allMappedReads, loci, args)
  }
}
