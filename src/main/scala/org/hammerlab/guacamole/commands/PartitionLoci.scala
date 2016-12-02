package org.hammerlab.guacamole.commands

import org.apache.spark.SparkContext
import org.hammerlab.commands.{ Args, SparkCommand }
import org.hammerlab.guacamole.loci.partitioning.{ HalfWindowArgs, LociPartitionerArgs, LociPartitioning }
import org.hammerlab.guacamole.readsets.ReadSets
import org.hammerlab.guacamole.readsets.args.{ Arguments => ReadSetsArgs }

class PartitionLociArgs
  extends Args
    with LociPartitionerArgs
    with HalfWindowArgs
    with ReadSetsArgs

object PartitionLoci extends SparkCommand[PartitionLociArgs] {

  override def name: String = "partition-loci"
  override def description: String = "Partition loci according to various paramters."

  override def run(args: PartitionLociArgs, sc: SparkContext): Unit = {
    val (readsets, loci) = ReadSets(sc, args)

    LociPartitioning(readsets.allMappedReads, loci, args)
  }
}
