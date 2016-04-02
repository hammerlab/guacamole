package org.hammerlab.guacamole.main

import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.Logging
import org.bdgenomics.utils.cli.Args4j
import org.hammerlab.guacamole._
import org.hammerlab.guacamole.distributed.LociPartitionUtils
import org.hammerlab.guacamole.reads.Read.InputFilters
import org.hammerlab.guacamole.reference.{ContigNotFound, ReferenceBroadcast}
import org.kohsuke.args4j.{Argument, Option => Args4jOption}

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
 */
object GeneratePartialFasta extends Logging {

  protected class Arguments extends LociPartitionUtils.Arguments with Common.Arguments.ReadLoadingConfigArgs with Common.Arguments.Reference {
    @Args4jOption(name = "--output", metaVar = "OUT", required = true, aliases = Array("-o"),
      usage = "Output path for partial fasta")
    var output: String = ""

    @Args4jOption(name = "--reference-fasta", required = true, usage = "Local path to a reference FASTA file")
    var referenceFastaPath: String = null

    @Argument(required = true, multiValued = true,
      usage = "Reads to write out overlapping fasta sequence for")
    var bams: Array[String] = Array.empty
  }

  def main(rawArgs: Array[String]): Unit = {
    val args = Args4j[Arguments](rawArgs)
    val sc = Common.createSparkContext(appName = "generate-partial-fasta")

    val reference = ReferenceBroadcast(args.referenceFastaPath, sc)
    val lociBuilder = Common.lociFromArguments(args, default = "none")
    val readsets =
      ReadSets(
        sc,
        args.bams,
        InputFilters.empty,
        config = Common.Arguments.ReadLoadingConfigArgs.fromArguments(args)
      )

    val reads = sc.union(readsets.mappedReads)

    val regions = reads.map(read => (read.referenceContig, read.start, read.end))
    regions.collect.foreach(triple => {
      lociBuilder.put(triple._1, triple._2, triple._3)
    })

    val loci = lociBuilder.result

    val fd = new File(args.output)
    val writer = new BufferedWriter(new FileWriter(fd))
    loci.contigs.foreach(contig => {
      loci.onContig(contig).ranges.foreach(range => {
        try {
          val sequence = Bases.basesToString(reference.getContig(contig).slice(range.start.toInt, range.end.toInt))
          writer.write(">%s:%d-%d/%d\n".format(contig, range.start, range.end, readsets.contigLengths(contig)))
          writer.write(sequence)
          writer.write("\n")
        } catch {
          case e: ContigNotFound => log.warn("No such contig in reference: %s: %s".format(contig, e.toString))
        }
      })
    })
    writer.close()
    Common.progress("Wrote: %s".format(args.output))
  }

  private def printUsage() = {
    println("Usage: java ... bam1.bam ... bamN.bam --output result.partial.fasta \n")
    println(
      """
        |Given a full fasta and some loci (either specified directly or from reads), write out a fasta containing only
        |the subset of the reference overlapped by the loci in a guacamole -specific fasta format
      """.trim.stripMargin)
  }
}
