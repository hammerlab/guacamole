package org.hammerlab.guacamole.commands.jointcaller

import htsjdk.samtools.SAMSequenceDictionary
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.Common.Arguments.NoSequenceDictionary
import org.hammerlab.guacamole._
import org.hammerlab.guacamole.commands.jointcaller.evidence.{MultiSampleMultiAlleleEvidence, MultiSampleSingleAlleleEvidence}
import org.hammerlab.guacamole.distributed.LociPartitionUtils
import org.hammerlab.guacamole.distributed.LociPartitionUtils.partitionLociAccordingToArgs
import org.hammerlab.guacamole.distributed.PileupFlatMapUtils.pileupFlatMapMultipleRDDs
import org.hammerlab.guacamole.loci.LociSet
import org.hammerlab.guacamole.reads._
import org.hammerlab.guacamole.reference.ReferenceBroadcast
import org.kohsuke.args4j.{Option => Args4jOption}

object SomaticJoint {
  class Arguments extends Parameters.CommandlineArguments with LociPartitionUtils.Arguments with NoSequenceDictionary with InputCollection.Arguments {
    @Args4jOption(name = "--out", usage = "Output path for all variants in VCF. Default: no output")
    var out: String = ""

    @Args4jOption(name = "--out-dir",
      usage = "Output dir for all variants, split into separate files for somatic/germline")
    var outDir: String = ""

    @Args4jOption(name = "--reference-fasta", required = true, usage = "Local path to a reference FASTA file")
    var referenceFastaPath: String = null

    @Args4jOption(name = "--reference-fasta-is-partial", usage = "Treat the reference fasta as a partial reference")
    var referenceFastaIsPartial: Boolean = false

    @Args4jOption(name = "--force-call-loci-from-file", usage = "Always call the given sites")
    var forceCallLociFromFile: String = ""

    @Args4jOption(name = "--force-call-loci", usage = "Always call the given sites")
    var forceCallLoci: String = ""

    @Args4jOption(name = "--only-somatic", usage = "Output only somatic calls, no germline calls")
    var onlySomatic: Boolean = false

    @Args4jOption(name = "--include-filtered", usage = "Include filtered calls")
    var includeFiltered: Boolean = false

    @Args4jOption(name = "-q", usage = "Quiet: less stdout")
    var quiet: Boolean = false
  }

  /**
   * Load ReadSet instances from user-specified BAMs (specified as an InputCollection).
   */
  def inputsToReadSets(sc: SparkContext,
                       inputs: InputCollection,
                       loci: LociSet.Builder,
                       contigLengthsFromDictionary: Boolean = true): ReadSets = {
    ReadSets(
      sc,
      inputs.items.map(_.path),
      Read.InputFilters(overlapsLoci = Some(loci)),
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

      val loci = Common.lociFromArguments(args)

      val ReadSets(readsRDDs, sequenceDictionary, contigLengths) =
        inputsToReadSets(sc, inputs, loci, !args.noSequenceDictionary)

      val forceCallLoci = if (args.forceCallLoci.nonEmpty || args.forceCallLociFromFile.nonEmpty) {
        Common.loci(args.forceCallLoci, args.forceCallLociFromFile, contigLengths)
      } else {
        LociSet.empty
      }

      if (forceCallLoci.nonEmpty) {
        Common.progress(
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
        readsRDDs,
        parameters,
        reference,
        loci.result(contigLengths),
        forceCallLoci = forceCallLoci,
        onlySomatic = args.onlySomatic,
        includeFiltered = args.includeFiltered,
        distributedUtilArguments = args
      )

      calls.cache()

      Common.progress("Collecting evidence for %,d sites with calls".format(calls.count))
      val collectedCalls = calls.collect()

      Common.progress("Called %,d germline and %,d somatic variants.".format(
        collectedCalls.count(_.singleAlleleEvidences.exists(_.isGermlineCall)),
        collectedCalls.count(_.singleAlleleEvidences.exists(_.isSomaticCall))))

      writeCalls(
        collectedCalls,
        inputs,
        parameters,
        sequenceDictionary.toSAMSequenceDictionary,
        forceCallLoci,
        reference,
        onlySomatic = args.onlySomatic,
        out = args.out,
        outDir = args.outDir
      )
    }
  }

  /** Subtract 1 from all loci in a LociSet. */
  def lociSetMinusOne(loci: LociSet): LociSet = {
    val builder = LociSet.newBuilder
    loci.contigs.foreach(contig => {
      val contigSet = loci.onContig(contig)
      contigSet.ranges.foreach(range => {
        builder.put(contig, math.max(0, range.start - 1), range.end - 1)
      })
    })
    builder.result
  }

  def makeCalls(sc: SparkContext,
                inputs: InputCollection,
                readsRDDs: PerSample[ReadsRDD],
                parameters: Parameters,
                reference: ReferenceBroadcast,
                loci: LociSet,
                forceCallLoci: LociSet = LociSet.empty,
                onlySomatic: Boolean = false,
                includeFiltered: Boolean = false,
                distributedUtilArguments: LociPartitionUtils.Arguments = new LociPartitionUtils.Arguments {}): RDD[MultiSampleMultiAlleleEvidence] = {

    // When mapping over pileups, at locus x we call variants at locus x + 1. Therefore we subtract 1 from the user-
    // specified loci.
    val broadcastForceCallLoci = sc.broadcast(forceCallLoci)

    val mappedReadRDDs = readsRDDs.map(_.mappedReads)

    val lociPartitions =
      partitionLociAccordingToArgs(
        distributedUtilArguments,
        lociSetMinusOne(loci),
        mappedReadRDDs: _*
      )

    pileupFlatMapMultipleRDDs(
      mappedReadRDDs,
      lociPartitions,
      skipEmpty = true,  // TODO: shouldn't skip empty positions if we might force call them. Need an efficient way to handle this.
      rawPileups => {
        val forceCall =
          broadcastForceCallLoci.value.onContig(rawPileups.head.referenceName)
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
                 forceCallLoci: LociSet = LociSet.empty,
                 reference: ReferenceBroadcast,
                 onlySomatic: Boolean = false,
                 out: String = "",
                 outDir: String = ""): Unit = {

    def writeSome(out: String,
                  filteredCalls: Seq[MultiSampleMultiAlleleEvidence],
                  filteredInputs: PerSample[Input],
                  includePooledNormal: Option[Boolean] = None,
                  includePooledTumor: Option[Boolean] = None): Unit = {

      val actuallyIncludePooledNormal = includePooledNormal.getOrElse(filteredInputs.count(_.normalDNA) > 1)
      val actuallyIncludePooledTumor = includePooledTumor.getOrElse(filteredInputs.count(_.tumorDNA) > 1)

      val numPooled = Seq(actuallyIncludePooledNormal, actuallyIncludePooledTumor).count(identity)
      val extra = if (numPooled > 0) "(plus %d pooled samples)".format(numPooled) else ""
      Common.progress("Writing %,d calls across %,d samples %s to %s".format(
        filteredCalls.length, filteredInputs.length, extra, out))

      VCFOutput.writeVcf(
        path = out,
        calls = filteredCalls,
        inputs = InputCollection(filteredInputs),
        includePooledNormal = actuallyIncludePooledNormal,
        includePooledTumor = actuallyIncludePooledTumor,
        parameters = parameters,
        sequenceDictionary = sequenceDictionary,
        reference = reference)
      Common.progress("Done.")
    }

    if (out.nonEmpty) {
      writeSome(out, calls, inputs.items)
    }
    if (outDir.nonEmpty) {
      def path(filename: String) = outDir + "/" + filename + ".vcf"
      def anyForced(evidence: MultiSampleSingleAlleleEvidence): Boolean = {
        forceCallLoci.onContig(evidence.allele.referenceContig)
          .intersects(evidence.allele.start, evidence.allele.end)
      }

      val dir = new java.io.File(outDir)
      val dirCreated = dir.mkdir()
      if (dirCreated) {
        Common.progress("Created directory: %s".format(dir))
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
