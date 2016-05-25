package org.hammerlab.guacamole.main

import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.SparkContext
import org.hammerlab.guacamole.commands.SparkCommand
import org.hammerlab.guacamole.distributed.LociPartitionUtils
import org.hammerlab.guacamole.loci.SimpleRange
import org.hammerlab.guacamole.logging.LoggingUtils.progress
import org.hammerlab.guacamole.readsets.{InputFilters, ReadLoadingConfig, ReadLoadingConfigArgs, ReadSets}
import org.hammerlab.guacamole.reference.{ContigNotFound, ReferenceArgs, ReferenceBroadcast}
import org.hammerlab.guacamole.util.Bases
import org.kohsuke.args4j.{Argument, Option => Args4jOption}

class GeneratePartialFastaArguments
  extends LociPartitionUtils.Arguments
    with ReadLoadingConfigArgs
    with ReferenceArgs {

  @Args4jOption(name = "--output", metaVar = "OUT", required = true, aliases = Array("-o"),
    usage = "Output path for partial fasta")
  var output: String = ""

  @Args4jOption(name = "--reference-fasta", required = true, usage = "Local path to a reference FASTA file")
  var referenceFastaPath: String = null

  @Args4jOption(name = "--padding", required = false, usage = "Number of bases to pad the reference around the loci")
  var padding: Int = 0

  @Argument(required = true, multiValued = true, usage = "Reads to write out overlapping fasta sequence for")
  var bams: Array[String] = Array.empty
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
 *   mvn package -DskipTests
 *   mvn test-compile
 *   java \
 *     -cp target/guacamole-with-dependencies-0.0.1-SNAPSHOT.jar:target/scala-2.10.5/test-classes \
 *     org.hammerlab.guacamole.main.GeneratePartialFasta \
 *     -o <output path> \
 *     --reference-fasta <fasta path> \
 *     <bam path> [bam path...]
 */
object GeneratePartialFasta extends SparkCommand[GeneratePartialFastaArguments] {

  override val name: String = "generate-partial-fasta"
  override val description: String = "generate \"partial fasta\"s for use in our tests of variant callers"

  def main(args: Array[String]): Unit = run(args)

  override def run(args: GeneratePartialFastaArguments, sc: SparkContext): Unit = {

    val reference = ReferenceBroadcast(args.referenceFastaPath, sc)
    val parsedLoci = args.parseLoci(sc.hadoopConfiguration, fallback = "none")
    val readsets =
      ReadSets(
        sc,
        args.bams,
        InputFilters.empty,
        config = ReadLoadingConfig(args)
      )

    val reads = sc.union(readsets.mappedReads)

    val regions = reads.map(read => (read.referenceContig, read.start, read.end))
    regions.collect.foreach(triple => {
      parsedLoci.put(triple._1, triple._2, triple._3)
    })

    val loci = parsedLoci.result

    val fd = new File(args.output)
    val writer = new BufferedWriter(new FileWriter(fd))

    val padding = args.padding
    for {
      contig <- loci.contigs
      SimpleRange(start, end) <- contig.ranges
    } {
      try {
        val paddedStart = start.toInt - padding
        val paddedEnd = end.toInt + padding
        val sequence = Bases.basesToString(reference.getContig(contig.name).slice(paddedStart, paddedEnd))
        writer.write(">%s:%d-%d/%d\n".format(contig.name, paddedStart, paddedEnd, readsets.contigLengths(contig.name)))
        writer.write(sequence)
        writer.write("\n")
      } catch {
        case e: ContigNotFound => log.warn("No such contig in reference: %s: %s".format(contig, e.toString))
      }
    }
    writer.close()
    progress(s"Wrote: ${args.output}")
  }
}
