package org.hammerlab.guacamole.commands.jointcaller

import htsjdk.samtools.SAMSequenceDictionary
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.Common.Arguments.NoSequenceDictionary
import org.hammerlab.guacamole._
import org.hammerlab.guacamole.reads._
import org.hammerlab.guacamole.reference.ReferenceBroadcast
import org.kohsuke.args4j.{ Option => Args4jOption }

object SomaticJoint {
  class Arguments extends Parameters.CommandlineArguments with DistributedUtil.Arguments with NoSequenceDictionary with InputCollection.Arguments {
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

    @Args4jOption(name = "-q", usage = "Quiet: less stdout")
    var quiet: Boolean = false
  }

  /**
   * Load ReadSet instances from user-specified BAMs (specified as an InputCollection).
   */
  def inputsToReadSets(sc: SparkContext,
                       inputs: InputCollection,
                       loci: LociSet.Builder,
                       reference: ReferenceBroadcast,
                       contigLengthsFromDictionary: Boolean = true): Seq[ReadSet] = {
    inputs.items.zipWithIndex.map({
      case (input, index) => ReadSet(
        sc,
        input.path,
        Read.InputFilters(overlapsLoci = Some(loci)),
        token = index,
        contigLengthsFromDictionary = contigLengthsFromDictionary,
        reference = reference)
    })
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

      val readSets = inputsToReadSets(sc, inputs, loci, reference, !args.noSequenceDictionary)

      assert(readSets.forall(_.sequenceDictionary == readSets(0).sequenceDictionary),
        "Samples have different sequence dictionaries: %s."
          .format(readSets.map(_.sequenceDictionary.toString).mkString("\n")))

      val forceCallLoci = if (args.forceCallLoci.nonEmpty || args.forceCallLociFromFile.nonEmpty) {
        Common.loci(args.forceCallLoci, args.forceCallLociFromFile, readSets(0))
      } else {
        LociSet.empty
      }

      if (forceCallLoci.nonEmpty) {
        Common.progress("Force calling %,d loci across %,d contig(s): %s".format(
          forceCallLoci.count,
          forceCallLoci.contigs.length,
          forceCallLoci.truncatedString()))
      }

      val parameters = Parameters(args)

      val allCalls = makeCalls(
        sc,
        inputs,
        readSets,
        parameters,
        reference,
        loci.result(readSets(0).contigLengths),
        forceCallLoci,
        args)

      val calls = allCalls.map(_.onlyBest)
      calls.cache()

      Common.progress("Collecting evidence for %,d sites with calls".format(calls.count))
      val collectedCalls = calls.collect()

      Common.progress("Called %,d germline and %,d somatic variants.".format(
        collectedCalls.count(_.alleleEvidences.exists(_.isGermlineCall)),
        collectedCalls.count(_.alleleEvidences.exists(_.isSomaticCall))))

      writeCalls(
        collectedCalls,
        inputs,
        parameters,
        readSets(0).sequenceDictionary.get.toSAMSequenceDictionary,
        forceCallLoci,
        reference,
        out = args.out,
        outDir = args.outDir)
    }
  }

  /** Subtract 1 from all loci in a LociSet. */
  def lociSetMinusOne(loci: LociSet): LociSet = {
    val builder = LociSet.newBuilder
    loci.contigs.foreach(contig => {
      val contigSet = loci.onContig(contig)
      contigSet.ranges.foreach(range => {
        builder.put(contig, math.max(0, range.start - 1), Some(range.end - 1))
      })
    })
    builder.result
  }

  def makeCalls(sc: SparkContext,
                inputs: InputCollection,
                readSets: Seq[ReadSet],
                parameters: Parameters,
                reference: ReferenceBroadcast,
                loci: LociSet,
                forceCallLoci: LociSet = LociSet.empty,
                distributedUtilArguments: DistributedUtil.Arguments = new DistributedUtil.Arguments {}): RDD[MultipleAllelesEvidenceAcrossSamples] = {

    // When mapping over pileups, at locus x we call variants at locus x + 1. Therefore we subtract 1 from the user-
    // specified loci.
    val broadcastForceCallLoci = sc.broadcast(forceCallLoci)
    val lociPartitions = DistributedUtil.partitionLociAccordingToArgs(
      distributedUtilArguments,
      lociSetMinusOne(loci),
      readSets.map(_.mappedReads): _*)

    val calls = DistributedUtil.pileupFlatMapMultipleRDDs(
      readSets.map(_.mappedReads),
      lociPartitions,
      true, // skip empty. TODO: shouldn't skip empty positions if we might force call them. Need an efficient way to handle this.
      pileups => {
        val normalPileups = inputs.normalDNA.map(input => pileups(input.index))
        val tumorDNAPileups = inputs.tumorDNA.map(input => pileups(input.index))
        val forceCall = broadcastForceCallLoci.value.onContig(pileups(0).referenceName).contains(pileups(0).locus + 1)

        val contig = normalPileups.head.referenceName
        val locus = normalPileups.head.locus

        // We only call variants at a site if the reference base is a standard base (i.e. not N).
        if (Bases.isStandardBase(reference.getReferenceBase(contig, locus.toInt + 1))) {
          val possibleAlleles = AlleleAtLocus.variantAlleles(
            (inputs.normalDNA ++ inputs.tumorDNA).map(input => pileups(input.index)),
            anyAlleleMinSupportingReads = parameters.anyAlleleMinSupportingReads,
            anyAlleleMinSupportingPercent = parameters.anyAlleleMinSupportingPercent,
            maxAlleles = Some(parameters.maxAllelesPerSite),
            atLeastOneAllele = forceCall, // if force calling this site, always get at least one allele
            onlyStandardBases = true)

          if (forceCall) assert(possibleAlleles.nonEmpty)

          if (possibleAlleles.nonEmpty) {
            val evidences = possibleAlleles.map(allele => {
              AlleleEvidenceAcrossSamples(
                parameters,
                allele,
                pileups,
                inputs)
            })
            if (forceCall || evidences.exists(_.isCall)) {
              val groupedEvidence = MultipleAllelesEvidenceAcrossSamples(evidences)
              Iterator(groupedEvidence)
            } else {
              Iterator.empty
            }
          } else {
            Iterator.empty
          }
        } else {
          Iterator.empty
        }
      }, reference = reference)
    calls
  }

  def writeCalls(calls: Seq[MultipleAllelesEvidenceAcrossSamples],
                 inputs: InputCollection,
                 parameters: Parameters,
                 sequenceDictionary: SAMSequenceDictionary,
                 forceCallLoci: LociSet = LociSet.empty,
                 reference: ReferenceBroadcast,
                 out: String = "",
                 outDir: String = ""): Unit = {

    def writeSome(out: String,
                  filteredCalls: Seq[MultipleAllelesEvidenceAcrossSamples],
                  filteredInputs: Seq[Input],
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
      def anyForced(evidence: AlleleEvidenceAcrossSamples): Boolean = {
        forceCallLoci.onContig(evidence.allele.referenceContig)
          .intersects(evidence.allele.start, evidence.allele.end)
      }

      val dir = new java.io.File(outDir)
      val dirCreated = dir.mkdir()
      if (dirCreated) {
        Common.progress("Created directory: %s".format(dir))
      }

      writeSome(path("all"), calls, inputs.items)

      writeSome(
        path("germline"),
        calls.filter(_.alleleEvidences.exists(
          evidence => evidence.isGermlineCall || anyForced(evidence))),
        inputs.items)

      val somaticCallsOrForced =
        calls.filter(_.alleleEvidences.exists(
          evidence => evidence.isSomaticCall || anyForced(evidence)))
      writeSome(path("somatic.all_samples"), somaticCallsOrForced, inputs.items)

      inputs.items.foreach(input => {
        writeSome(
          path("all.%s.%s.%s".format(
            input.sampleName, input.tissueType.toString, input.analyte.toString)),
          calls,
          Seq(input))
        writeSome(
          path("somatic.%s.%s.%s".format(
            input.sampleName, input.tissueType.toString, input.analyte.toString)),
          somaticCallsOrForced,
          Seq(input))
      })
      writeSome(path("all.tumor_pooled_dna"), somaticCallsOrForced, Seq.empty, includePooledTumor = Some(true))
    }
  }
}
