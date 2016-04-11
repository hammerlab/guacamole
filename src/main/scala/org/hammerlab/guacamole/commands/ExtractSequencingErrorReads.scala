package org.hammerlab.guacamole.commands

import java.io.{BufferedWriter, FileWriter}

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.{GaussianMixture, GaussianMixtureModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.hammerlab.guacamole._
import org.hammerlab.guacamole.distributed.LociPartitionUtils
import org.hammerlab.guacamole.distributed.PileupFlatMapUtils._
import org.hammerlab.guacamole.loci.LociMap
import org.hammerlab.guacamole.pileup.Pileup
import org.hammerlab.guacamole.reads.Read.InputFilters
import org.hammerlab.guacamole.reads.{MappedRead, PairedRead, Read}
import org.hammerlab.guacamole.reference.{ReferenceBroadcast, ReferenceGenome}
import org.kohsuke.args4j.{Argument, Option => Args4jOption}

object ExtractSequencingErrorReads {

  // TODO: include original qualities
  case class ResultRow(inputIndex: Int,
                       isReference: Boolean,
                       contig: String,
                       startPosition: Long,
                       readSequence: String,
                       qualities: Seq[Byte],
                       isPositiveStrand: Boolean,
                       isFirstInPair: Boolean) {

    override def toString: String = {
      val stringQualities = qualities.map(_.toInt.toString).mkString(" ")
      s"${inputIndex}, ${isReference}, ${contig}, ${startPosition}, ${readSequence}, ${stringQualities}, ${isPositiveStrand}, ${isFirstInPair}"
    }
  }

  object ResultRow {
    val header = Seq("input, isReference, contig, startPosition, readSequence, qualities, isPositiveStrand, isFirstInPair")

    def apply(inputIndex: Int, isReference: Boolean, read: MappedRead): ResultRow = {
      ResultRow(
        inputIndex,
        isReference,
        read.referenceContig,
        read.start,
        Bases.basesToString(read.sequence),
        read.baseQualities,
        read.isPositiveStrand,
        read.isPaired && (read.asInstanceOf[PairedRead[MappedRead]].isFirstInPair)
      )
    }
  }

  protected class Arguments extends LociPartitionUtils.Arguments with Common.Arguments.ReadLoadingConfigArgs {
    @Args4jOption(name = "--out", required = true,
      usage = "Local file path to save the variant allele frequency histogram")
    var out: String = ""

    @Args4jOption(name = "--min-read-depth", usage = "Minimum read depth to include")
    var minReadDepth: Int = 30

    @Args4jOption(name = "--reference-fasta", required = true, usage = "Local path to a reference FASTA file")
    var referenceFastaPath: String = ""

    @Args4jOption(name = "--reference-fasta-is-partial", usage = "Treat the reference fasta as a partial reference")
    var referenceFastaIsPartial: Boolean = false

    @Argument(required = true, multiValued = true, usage = "BAMs")
    var bams: Array[String] = Array.empty
  }

  object Caller extends SparkCommand[Arguments] {
    override val name = "extract-sequencing-error-reads"
    override val description = ""

    override def run(args: Arguments, sc: SparkContext): Unit = {
      val reference = ReferenceBroadcast(args.referenceFastaPath, sc, partialFasta = args.referenceFastaIsPartial)

      val loci = Common.lociFromArguments(args)
      val filters = Read.InputFilters(
        overlapsLoci = Some(loci),
        isPaired = true,
        passedVendorQualityChecks = true)

      val readSets: Seq[ReadSet] = args.bams.zipWithIndex.map(
        bamFile =>
          ReadSet(
            sc,
            bamFile._1,
            InputFilters.empty,
            contigLengthsFromDictionary = true,
            config = Common.Arguments.ReadLoadingConfigArgs.fromArguments(args)
          )
      )

      val lociPartitions = LociPartitionUtils.partitionLociAccordingToArgs(
        args,
        loci.result(readSets(0).contigLengths),
        readSets(0).mappedReads // Use the first set of reads as a proxy for read depth
      )

      val minReadDepth = args.minReadDepth
      val rows = readSets.zipWithIndex.flatMap(pair => {
        val index = pair._2
        process(pair._1.mappedReads, index, lociPartitions, args.minReadDepth, reference)
      })

      if (args.out != "") {
        Common.progress("Writing: %s".format(args.out))
        val writer = new BufferedWriter(new FileWriter(args.out))
        writer.write(ResultRow.header.mkString(", "))
        writer.newLine()
        rows.foreach(row => {
          writer.write(row.toString)
          writer.newLine()
        })
        writer.flush()
        writer.close()
        Common.progress("Done")
      }
    }
  }


  def process(reads: RDD[MappedRead], index: Int, loci: LociMap[Long], minReadDepth: Int, reference: ReferenceGenome): Seq[ResultRow] = {
    val results = pileupFlatMap[ResultRow](
      reads,
      loci,
      true,
      pileup => {
        extractRows(index, pileup, minReadDepth)
      },
      reference = reference)
      results.collect
  }

  def extractRows(index: Int, pileup: Pileup, minReadDepth: Int = 0): Iterator[ResultRow] = {
    if (pileup.elements.length < minReadDepth || pileup.elements.count(_.read.alignmentQuality == 0) >= 2) {
      return Iterator.empty
    }

    val nonDuplicates = pileup.elements.filter(_.read.isDuplicate)
    if (nonDuplicates.length < minReadDepth || nonDuplicates.exists(!_.isMatch)) {
      return Iterator.empty
    }

    val mismatchingDuplicates =  pileup.elements.filter(!_.isMatch)
    if (mismatchingDuplicates.isEmpty) {
      return Iterator.empty
    }

    val groupedMismatchingDuplicates = mismatchingDuplicates.groupBy(_.read.start)
    if (groupedMismatchingDuplicates.exists(_._2.length > 1)) {
      // Single fragment with multiple errors, seems suspicious may be pcr error
      return Iterator.empty
    }

    val matchingNonDuplicatesByStart = pileup.elements
      .filter(e => !e.read.isDuplicate && e.isMatch)
      .map(e => e.read.start -> e.read)
      .toMap

    groupedMismatchingDuplicates
      .filterKeys(start => matchingNonDuplicatesByStart.contains(start))
      .flatMap(pair => {
        val mismatchingRead = pair._2.head.read
        val mismatchingRow = ResultRow(index, false, mismatchingRead)
        val matchingRead = matchingNonDuplicatesByStart(pair._1)
        val matchingRow = ResultRow(index, true, matchingRead)
        Iterator(matchingRow, mismatchingRow)
      }).toIterator
  }
}

