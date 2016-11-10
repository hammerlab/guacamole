package org.hammerlab.guacamole.main

import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.SparkContext
import org.hammerlab.guacamole.commands.{Args, SparkCommand}
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.logging.LoggingUtils.progress
import org.hammerlab.guacamole.readsets.ReadSets
import org.hammerlab.guacamole.readsets.args.{ReferenceArgs, Arguments => ReadSetsArguments}
import org.hammerlab.guacamole.readsets.io.InputConfig
import org.hammerlab.guacamole.readsets.rdd.PartitionedRegionsArgs
import org.hammerlab.guacamole.reference.{ContigNotFound, Interval}
import org.hammerlab.guacamole.util.Bases.basesToString
import org.kohsuke.args4j.{Option => Args4jOption}

class GeneratePartialFastaArguments
  extends Args
    with ReadSetsArguments
    with PartitionedRegionsArgs
    with ReferenceArgs {

  @Args4jOption(name = "--out", metaVar = "OUT", required = true, aliases = Array("-o"),
    usage = "Output path for partial fasta")
  var output: String = ""

  @Args4jOption(name = "--padding", required = false, usage = "Number of bases to pad the reference around the loci")
  var padding: Int = 0
}

/**
 * This command is used to generate a "partial fasta" which we use in our tests of variant callers. It should be run
 * locally. It is not intended for any large-scale or production use.
 *
 * A "partial fasta" is a fasta file where the reference names look like "chr1:9242255-9242454/249250621". That gives
 * the contig name, the start and end locus, and the total contig size. The associated sequence in the file gives the
 * reference sequence for just the sites between start and end.
 *
 * This lets us package up a subset of a reference fasta into a file that is small enough to version control and
 * distribute.
 *
 * To run this command, build the main Guacamole package, compile test-classes, and run this class with both the
 * assembly JAR and test-classes on the classpath:
 *
 *   mvn package -DskipTests -Pguac,test
 *   scripts/guacamole-test \
 *     GeneratePartialFasta \
 *     --reference <fasta path> \
 *     [--loci <str>|--loci-file <file>] \
 *     -o <output path> \
 *     <bam path> [bam path...]
 */
object GeneratePartialFasta extends SparkCommand[GeneratePartialFastaArguments] {

  override val name: String = "generate-partial-fasta"
  override val description: String = "generate \"partial fasta\"s for use in our tests of variant callers"

  def main(args: Array[String]): Unit = run(args)

  override def run(args: GeneratePartialFastaArguments, sc: SparkContext): Unit = {

    val reference = args.reference(sc)
    val parsedLoci = args.parseConfig(sc.hadoopConfiguration).loci

    val readsets =
      ReadSets(
        sc,
        args.inputs,
        InputConfig.empty
      )

    val contigLengths = readsets.contigLengths

    val loci =
      LociSet(
        readsets
          .allMappedReads
          .map(read => (read.contigName, read.start, read.end))
          .collect
      )

    val fd = new File(args.output)
    val writer = new BufferedWriter(new FileWriter(fd))

    val padding = args.padding
    for {
      contig <- loci.contigs
      Interval(start, end) <- contig.ranges
    } {
      try {
        val paddedStart = start.toInt - padding
        val paddedEnd = end.toInt + padding
        val sequence = basesToString(reference.getContig(contig.name).slice(paddedStart, paddedEnd))
        writer.write(">%s:%d-%d/%d\n".format(contig.name, paddedStart, paddedEnd, contigLengths(contig.name)))
        writer.write(sequence)
        writer.write("\n")
      } catch {
        case e: ContigNotFound => warn(s"No such contig in reference: $contig: $e")
      }
    }
    writer.close()
    progress(s"Wrote: ${args.output}")
  }
}
