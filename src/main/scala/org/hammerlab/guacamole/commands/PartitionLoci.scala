package org.hammerlab.guacamole.commands

import org.apache.spark.SparkContext
import org.hammerlab.cli.args4j.Args
import org.hammerlab.genomics.readsets.ReadSets
import org.hammerlab.genomics.readsets.args.impl.{ Arguments ⇒ ReadSetsArgs }
import org.hammerlab.guacamole.loci.partitioning.{ HalfWindowArgs, LociPartitionerArgs, LociPartitioning }

class PartitionLociArgs
  extends Args
    with LociPartitionerArgs
    with HalfWindowArgs
    with ReadSetsArgs

object PartitionLoci extends GuacCommand[PartitionLociArgs] {

  override def name: String = "partition-loci"
  override def description: String = "Partition loci according to various paramters."

  override def run(args: PartitionLociArgs, sc: SparkContext): Unit = {
    val (readsets, loci) = ReadSets(sc, args)

    LociPartitioning(readsets.allMappedReads, loci, args)
  }
}
