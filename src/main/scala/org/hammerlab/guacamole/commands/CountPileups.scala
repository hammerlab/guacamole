package org.hammerlab.guacamole.commands

import org.hammerlab.guacamole.distributed.PileupFlatMapUtils.pileupFlatMapMultipleRDDs
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.bdgenomics.utils.cli.Args4jBase
import org.hammerlab.guacamole.loci.partitioning.LociPartitioning
import org.hammerlab.guacamole.pileup.PileupsRDD._
import org.hammerlab.guacamole.reads.MappedRead
import org.hammerlab.guacamole.readsets.rdd.PartitionedRegions
import org.hammerlab.guacamole.reference.ReferenceBroadcast
import org.kohsuke.args4j.Argument

class CountPileupsArgs extends Args4jBase {

  @Argument(
    metaVar = "reference",
    required = true,
    usage = "Path to reference"
  )
  var reference: String = null

  @Argument(
    metaVar = "partitioningDir",
    required = true,
    usage = "Directory from which to read an existing partition-reads RDD, with accompanying LociMap partitioning.",
    index = 1
  )
  var partitioningDir: String = null

  @Argument(
    metaVar = "strategy",
    required = false,
    usage = "Pileup-construction strategy.",
    index = 2
  )
  var pileupStrategy: String = "iterator"
}

/**
 * Simple command for building Pileups from read sets; useful for benchmarking loci/pileup strategies.
 */
object CountPileups extends SparkCommand[CountPileupsArgs] {
  override def run(args: CountPileupsArgs, sc: SparkContext): Unit = {
    val hc = sc.hadoopConfiguration
    val fs = FileSystem.get(hc)
    val lp = LociPartitioning.load(fs.open(new Path(args.partitioningDir, "partitioning")))
    val pr = PartitionedRegions.load[MappedRead](sc, new Path(args.partitioningDir, "reads").toString, lp)

    val reference = ReferenceBroadcast(args.reference, sc, partialFasta = true)

    println(s"pileup strategy: ${args.pileupStrategy}")

    val pileups =
      if (args.pileupStrategy == "iterator") {
        pr.perSamplePileups(
          numSamples = 3,
          reference = reference
        )
      } else if (args.pileupStrategy == "windows") {
        pileupFlatMapMultipleRDDs(
          numSamples = 3,
          pr,
          skipEmpty = false,
          pileups => Iterator(pileups),
          reference
        )
      } else
        throw new IllegalArgumentException(s"Bad pileup strategy: ${args.pileupStrategy}")

    println(s"${pileups.count} pileups")
  }

  override val name: String = "count-pileups"
  override val description: String = "test cmd"
}
