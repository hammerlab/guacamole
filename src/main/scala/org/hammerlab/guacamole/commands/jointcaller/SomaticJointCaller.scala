package org.hammerlab.guacamole.commands.jointcaller

import htsjdk.samtools.SAMSequenceDictionary
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.commands.SparkCommand
import org.hammerlab.guacamole.commands.jointcaller.evidence.{MultiSampleMultiAlleleEvidence, MultiSampleSingleAlleleEvidence}
import org.hammerlab.guacamole.distributed.PileupFlatMapUtils.pileupFlatMapMultipleRDDs
import org.hammerlab.guacamole.loci.LociArgs
import org.hammerlab.guacamole.loci.partitioning.LociPartitionerArgs
import org.hammerlab.guacamole.loci.set.{LociParser, LociSet}
import org.hammerlab.guacamole.logging.LoggingUtils.progress
import org.hammerlab.guacamole.readsets.{InputFilters, NoSequenceDictionaryArgs, PerSample, ReadSets}
import org.hammerlab.guacamole.reference.ReferenceBroadcast
import org.kohsuke.args4j.spi.StringArrayOptionHandler
import org.kohsuke.args4j.{Option => Args4jOption}

object SomaticJoint {
  class Arguments
    extends Parameters.CommandlineArguments
      with LociPartitionerArgs
      with NoSequenceDictionaryArgs
      with InputCollection.Arguments {

    @Args4jOption(name = "--out", usage = "Output path for all variants in VCF. Default: no output")
    var out: String = ""

    @Args4jOption(name = "--out-dir",
      usage = "Output dir for all variants, split into separate files for somatic/germline")
    var outDir: String = ""

    @Args4jOption(name = "--reference-fasta", required = true, usage = "Local path to a reference FASTA file")
    var referenceFastaPath: String = null

    @Args4jOption(name = "--reference-fasta-is-partial", usage = "Treat the reference fasta as a partial reference")
    var referenceFastaIsPartial: Boolean = false

    @Args4jOption(name = "--force-call-loci-file", usage = "Always call the given sites")
    var forceCallLociFile: String = ""

    @Args4jOption(name = "--force-call-loci", usage = "Always call the given sites")
    var forceCallLoci: String = ""

    @Args4jOption(name = "--only-somatic", usage = "Output only somatic calls, no germline calls")
    var onlySomatic: Boolean = false

    @Args4jOption(name = "--include-filtered", usage = "Include filtered calls")
    var includeFiltered: Boolean = false

    @Args4jOption(name = "-q", usage = "Quiet: less stdout")
    var quiet: Boolean = false

    // For example:
    //  --header-metadata kind=tuning_test version=4
    @Args4jOption(name = "--header-metadata",
      usage = "Extra header metadata for VCF output in format KEY=VALUE KEY=VALUE ...",
      handler = classOf[StringArrayOptionHandler])
    var headerMetadata: Array[String] = Array.empty
  }

  /**
   * Load ReadSet instances from user-specified BAMs (specified as an InputCollection).
   */
  def inputsToReadSets(sc: SparkContext,
                       inputs: InputCollection,
                       loci: LociParser,
                       contigLengthsFromDictionary: Boolean = true): ReadSets = {
    ReadSets(
      sc,
      inputs.items.map(_.path),
      InputFilters(overlapsLoci = loci),
      contigLengthsFromDictionary = contigLengthsFromDictionary
    )
  }

  object Caller extends SparkCommand[Arguments] {
    override val name = "somatic-joint"
    override val description = "call germline and somatic variants based on any number of samples from the same patient"

    override def run(args: Arguments, sc: SparkContext): Unit = {
      val inputs = InputCollection(args)

      if (!args.quiet) {
        println("Running on %d inputs:".format(inputs.items.length))
        inputs.items.foreach(input => println(input))
      }

      val reference = ReferenceBroadcast(args.referenceFastaPath, sc, partialFasta = args.referenceFastaIsPartial)

      val loci = args.parseLoci(sc.hadoopConfiguration)

      val readsets = inputsToReadSets(sc, inputs, loci, !args.noSequenceDictionary)

      val contigLengths = readsets.contigLengths

      val forceCallLoci =
        LociArgs.parseLoci(
          args.forceCallLoci,
          args.forceCallLociFile,
          sc.hadoopConfiguration,
          fallback = ""
        ).result(contigLengths)

      if (forceCallLoci.nonEmpty) {
        progress(
          "Force calling %,d loci across %,d contig(s): %s".format(
            forceCallLoci.count,
            forceCallLoci.contigs.length,
            forceCallLoci.truncatedString()
          )
        )
      }

      val parameters = Parameters(args)

      val calls = makeCalls(
        sc,
        inputs,
        readsets,
        parameters,
        reference,
        loci.result(contigLengths),
        forceCallLoci,
        args.onlySomatic,
        args.includeFiltered,
        args
      )

      calls.cache()

      progress("Collecting evidence for %,d sites with calls".format(calls.count))
      val collectedCalls = calls.collect()

      progress(
        "Called %,d germline and %,d somatic variants.".format(
          collectedCalls.count(_.singleAlleleEvidences.exists(_.isGermlineCall)),
          collectedCalls.count(_.singleAlleleEvidences.exists(_.isSomaticCall))
        )
      )

      val extraHeaderMetadata = args.headerMetadata.map(value => {
        val split = value.split("=")
        if (split.length != 2) {
          throw new RuntimeException(s"Invalid header metadata item $value, expected KEY=VALUE")
        }
        (split(0), split(1))
      })

      writeCalls(
        collectedCalls,
        inputs,
        parameters,
        readsets.sequenceDictionary.toSAMSequenceDictionary,
        forceCallLoci,
        reference,
        onlySomatic = args.onlySomatic,
        out = args.out,
        outDir = args.outDir,
        extraHeaderMetadata = extraHeaderMetadata
      )
    }
  }

  /** Subtract 1 from all loci in a LociSet. */
  def lociSetMinusOne(loci: LociSet): LociSet = {
    LociSet(
      for {
        contig <- loci.contigs
        range <- contig.ranges
      } yield {
        (contig.name, math.max(0, range.start - 1), range.end - 1)
      }
    )
  }

  def makeCalls(sc: SparkContext,
                inputs: InputCollection,
                readsets: ReadSets,
                parameters: Parameters,
                reference: ReferenceBroadcast,
                loci: LociSet,
                forceCallLoci: LociSet = LociSet(),
                onlySomatic: Boolean = false,
                includeFiltered: Boolean = false,
                args: LociPartitionerArgs = new LociPartitionerArgs {}): RDD[MultiSampleMultiAlleleEvidence] = {

    assume(loci.nonEmpty)

    // When mapping over pileups, at locus x we call variants at locus x + 1. Therefore we subtract 1 from the user-
    // specified loci.
    val broadcastForceCallLoci = sc.broadcast(forceCallLoci)

    val lociPartitions =
      args
        .getPartitioner(readsets.allMappedReads)
        // When mapping over pileups, at locus x we call variants at locus x + 1. Therefore we subtract 1 from the user-
        // specified loci.
        .partition(lociSetMinusOne(loci))

    pileupFlatMapMultipleRDDs(
      readsets.mappedReadsRDDs,
      lociPartitions,
      skipEmpty = true,  // TODO: shouldn't skip empty positions if we might force call them. Need an efficient way to handle this.
      rawPileups => {
        val forceCall =
          broadcastForceCallLoci.value.onContig(rawPileups.head.contigName)
            .contains(rawPileups.head.locus + 1)

        MultiSampleMultiAlleleEvidence.make(
          rawPileups,
          inputs,
          parameters,
          reference,
          forceCall = forceCall,
          onlySomatic = onlySomatic,
          includeFiltered = includeFiltered).toIterator
      },
      reference = reference
    )
  }

  def writeCalls(calls: Seq[MultiSampleMultiAlleleEvidence],
                 inputs: InputCollection,
                 parameters: Parameters,
                 sequenceDictionary: SAMSequenceDictionary,
                 forceCallLoci: LociSet = LociSet(),
                 reference: ReferenceBroadcast,
                 onlySomatic: Boolean = false,
                 out: String = "",
                 outDir: String = "",
                 extraHeaderMetadata: Seq[(String, String)] = Seq.empty): Unit = {

    def writeSome(out: String,
                  filteredCalls: Seq[MultiSampleMultiAlleleEvidence],
                  filteredInputs: PerSample[Input],
                  includePooledNormal: Option[Boolean] = None,
                  includePooledTumor: Option[Boolean] = None): Unit = {

      val actuallyIncludePooledNormal = includePooledNormal.getOrElse(filteredInputs.count(_.normalDNA) > 1)
      val actuallyIncludePooledTumor = includePooledTumor.getOrElse(filteredInputs.count(_.tumorDNA) > 1)

      val numPooled = Seq(actuallyIncludePooledNormal, actuallyIncludePooledTumor).count(identity)
      val extra =
        if (numPooled > 0)
          s"(plus $numPooled pooled samples)"
        else
          ""

      progress(
        "Writing %,d calls across %,d samples %s to %s".format(
          filteredCalls.length, filteredInputs.length, extra, out
        )
      )

      VCFOutput.writeVcf(
        path = out,
        calls = filteredCalls,
        inputs = InputCollection(filteredInputs),
        includePooledNormal = actuallyIncludePooledNormal,
        includePooledTumor = actuallyIncludePooledTumor,
        parameters = parameters,
        sequenceDictionary = sequenceDictionary,
        reference = reference,
        extraHeaderMetadata = extraHeaderMetadata
      )
      progress("Done.")
    }

    if (out.nonEmpty) {
      writeSome(out, calls, inputs.items)
    }
    if (outDir.nonEmpty) {
      def path(filename: String) = outDir + "/" + filename + ".vcf"
      def anyForced(evidence: MultiSampleSingleAlleleEvidence): Boolean = {
        forceCallLoci.onContig(evidence.allele.contigName)
          .intersects(evidence.allele.start, evidence.allele.end)
      }

      val dir = new java.io.File(outDir)
      val dirCreated = dir.mkdir()
      if (dirCreated) {
        progress(s"Created directory: $dir")
      }

      writeSome(path("all"), calls, inputs.items)

      if (!onlySomatic) {
        writeSome(
          path("germline"),
          calls.filter(_.singleAlleleEvidences.exists(
            evidence => evidence.isGermlineCall || anyForced(evidence))),
          inputs.items)
      }

      val somaticCallsOrForced =
        calls.filter(_.singleAlleleEvidences.exists(
          evidence => evidence.isSomaticCall || anyForced(evidence)))
      writeSome(path("somatic.all_samples"), somaticCallsOrForced, inputs.items)

      inputs.items.foreach(input => {
        if (!onlySomatic) {
          writeSome(
            path("all.%s.%s.%s".format(
              input.sampleName, input.tissueType.toString, input.analyte.toString)),
            calls,
            Vector(input))
        }
        writeSome(
          path("somatic.%s.%s.%s".format(
            input.sampleName, input.tissueType.toString, input.analyte.toString)),
          somaticCallsOrForced,
          Vector(input))
      })
      writeSome(path("all.tumor_pooled_dna"), somaticCallsOrForced, Vector.empty, includePooledTumor = Some(true))
    }
  }
}
