package org.hammerlab.guacamole.commands

import htsjdk.samtools.SAMSequenceDictionary
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.hammerlab.args4s.PathOptionHandler
import org.hammerlab.commands.Args
import org.hammerlab.genomics.loci.parsing.ParsedLoci
import org.hammerlab.genomics.loci.set.LociSet
import org.hammerlab.genomics.readsets.args.impl.{ ReferenceArgs, Arguments ⇒ ReadSetsArguments }
import org.hammerlab.genomics.readsets.{ PerSample, ReadSets }
import org.hammerlab.genomics.reference.Region
import org.hammerlab.guacamole.distributed.PileupFlatMapUtils.pileupFlatMapMultipleSamples
import org.hammerlab.guacamole.jointcaller.Samples._
import org.hammerlab.guacamole.jointcaller.VCFOutput.writeVcf
import org.hammerlab.guacamole.jointcaller.evidence.{ MultiSampleMultiAlleleEvidence, MultiSampleSingleAlleleEvidence }
import org.hammerlab.guacamole.jointcaller.{ Inputs, Parameters, Samples }
import org.hammerlab.guacamole.loci.args.ForceCallLociArgs
import org.hammerlab.guacamole.logging.LoggingUtils.progress
import org.hammerlab.guacamole.pileup.Pileup
import org.hammerlab.guacamole.readsets.rdd.{ PartitionedRegions, PartitionedRegionsArgs }
import org.hammerlab.guacamole.reference.ReferenceBroadcast
import org.hammerlab.paths.Path
import org.kohsuke.args4j.spi.StringArrayOptionHandler
import org.kohsuke.args4j.{ Option ⇒ Args4jOption }

object SomaticJoint {
  class Arguments
    extends Args
      with ReadSetsArguments
      with PartitionedRegionsArgs
      with Parameters.CommandlineArguments
      with Inputs.Arguments
      with ForceCallLociArgs
      with ReferenceArgs {

    @Args4jOption(
      name = "--out",
      handler = classOf[PathOptionHandler],
      usage = "Output path for all variants in VCF. Default: no output"
    )
    var outOpt: Option[Path] = None

    @Args4jOption(
      name = "--out-dir",
      handler = classOf[PathOptionHandler],
      usage = "Output dir for all variants, split into separate files for somatic/germline"
    )
    var outDirOpt: Option[Path] = None

    @Args4jOption(name = "--only-somatic", usage = "Output only somatic calls, no germline calls")
    var onlySomatic: Boolean = false

    @Args4jOption(name = "--include-filtered", usage = "Include filtered calls")
    var includeFiltered: Boolean = false

    // For example:
    //  --header-metadata kind=tuning_test version=4
    @Args4jOption(name = "--header-metadata",
      usage = "Extra header metadata for VCF output in format KEY=VALUE KEY=VALUE ...",
      handler = classOf[StringArrayOptionHandler])
    var headerMetadata: Array[String] = Array.empty
  }

  object Caller extends GuacCommand[Arguments] {
    override val name = "somatic-joint"
    override val description = "call germline and somatic variants based on any number of samples from the same patient"

    override def run(args: Arguments, sc: SparkContext): Unit = {
      val inputs = Inputs(args)

      val (readsets, loci) = ReadSets(sc, args)

      info(
        (s"Running on ${inputs.length} inputs:" :: inputs.toList).mkString("\n")
      )

      val forceCallLoci =
        LociSet(
          ParsedLoci(
            args.forceCallLociStrOpt,
            args.forceCallLociFileOpt,
            sc.hadoopConfiguration
          )
          .getOrElse(ParsedLoci("")),
          readsets.contigLengths
        )

      if (forceCallLoci.nonEmpty) {
        progress(
          "Force calling %,d loci across %,d contig(s): %s".format(
            forceCallLoci.count.num,
            forceCallLoci.contigs.length,
            forceCallLoci.toString(10000)
          )
        )
      }

      val parameters = Parameters(args)

      val reference = ReferenceBroadcast(args, sc)

      val calls =
        makeCalls(
          sc,
          inputs,
          readsets,
          parameters,
          reference,
          loci,
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

      // User defined additional VCF headers, plus the Spark applicationId.
      val extraHeaderMetadata = args.headerMetadata.map(value ⇒ {
        val split = value.split("=")
        if (split.length != 2) {
          throw new RuntimeException(s"Invalid header metadata item $value, expected KEY=VALUE")
        }
        (split(0), split(1))
      }) ++ Seq(("applicationId", sc.applicationId))

      writeCalls(
        collectedCalls,
        inputs,
        parameters,
        readsets.sequenceDictionary.toSAMSequenceDictionary,
        forceCallLoci,
        reference,
        onlySomatic = args.onlySomatic,
        outOpt = args.outOpt,
        outDirOpt = args.outDirOpt,
        extraHeaderMetadata = extraHeaderMetadata
      )
    }
  }

  /** Subtract 1 from all loci in a LociSet. */
  def lociSetMinusOne(loci: LociSet): LociSet =
    LociSet(
      for {
        contig ← loci.contigs
        range ← contig.ranges
      } yield
        Region(
          contig.name,
          range.start.prev,
          range.end.prev
        )
    )

  def makeCalls(sc: SparkContext,
                inputs: Inputs,
                readsets: ReadSets,
                parameters: Parameters,
                reference: ReferenceBroadcast,
                loci: LociSet,
                forceCallLoci: LociSet = LociSet(),
                onlySomatic: Boolean = false,
                includeFiltered: Boolean = false,
                args: Arguments = new Arguments {}): RDD[MultiSampleMultiAlleleEvidence] = {

    assume(loci.nonEmpty)

    val partitionedReads =
      PartitionedRegions(
        readsets.sampleIdxKeyedMappedReads,
        lociSetMinusOne(loci),
        args
      )

    val broadcastForceCallLoci = sc.broadcast(forceCallLoci)

    val inputsBroadcast = sc.broadcast(inputs)

    def callPileups(pileups: PerSample[Pileup]): Iterator[MultiSampleMultiAlleleEvidence] = {
      val forceCall =
        broadcastForceCallLoci
          .value(pileups.head.contigName)
          .contains(pileups.head.locus.next)

      MultiSampleMultiAlleleEvidence(
        pileups,
        inputsBroadcast.value,
        parameters,
        reference,
        forceCall,
        onlySomatic,
        includeFiltered
      )
      .iterator
    }

    pileupFlatMapMultipleSamples(
      readsets.sampleNames,
      partitionedReads,
      skipEmpty = true,  // TODO: shouldn't skip empty positions if we might force call them. Need an efficient way to handle this.
      callPileups,
      reference = reference
    )
  }

  def writeCalls(calls: Seq[MultiSampleMultiAlleleEvidence],
                 samples: Samples,
                 parameters: Parameters,
                 sequenceDictionary: SAMSequenceDictionary,
                 forceCallLoci: LociSet = LociSet(),
                 reference: ReferenceBroadcast,
                 onlySomatic: Boolean = false,
                 outOpt: Option[Path],
                 outDirOpt: Option[Path],
                 extraHeaderMetadata: Seq[(String, String)] = Seq.empty): Unit = {

    def writeSome(out: Path,
                  filteredCalls: Seq[MultiSampleMultiAlleleEvidence],
                  samples: Samples,
                  includePooledNormal: Option[Boolean] = None,
                  includePooledTumor: Option[Boolean] = None): Unit = {

      val actuallyIncludePooledNormal = includePooledNormal.getOrElse(samples.count(_.normalDNA) > 1)
      val actuallyIncludePooledTumor = includePooledTumor.getOrElse(samples.count(_.tumorDNA) > 1)

      val numPooled = Seq(actuallyIncludePooledNormal, actuallyIncludePooledTumor).count(identity)
      val extra =
        if (numPooled > 0)
          s"(plus $numPooled pooled samples)"
        else
          ""

      progress(
        "Writing %,d calls across %,d samples %s to %s".format(
          filteredCalls.length, samples.length, extra, out
        )
      )

      writeVcf(
        path = out,
        calls = filteredCalls,
        samples = samples,
        includePooledNormal = actuallyIncludePooledNormal,
        includePooledTumor = actuallyIncludePooledTumor,
        parameters = parameters,
        sequenceDictionary = sequenceDictionary,
        reference = reference,
        extraHeaderMetadata = extraHeaderMetadata
      )
      progress("Done.")
    }

    outOpt.foreach(out ⇒ writeSome(out, calls, samples))

    outDirOpt foreach {
      outDir ⇒

        def path(filename: String) = outDir / s"$filename.vcf"

        def anyForced(evidence: MultiSampleSingleAlleleEvidence): Boolean =
          forceCallLoci.intersects(evidence.allele)

        val dirFile = outDir.toFile
        val dirCreated = dirFile.mkdir()
        if (dirCreated) {
          progress(s"Created directory: $outDir")
        }

        writeSome(path("all"), calls, samples)

        if (!onlySomatic)
          writeSome(
            path("germline"),
            calls.filter(
              _.singleAlleleEvidences.exists(
                evidence ⇒ evidence.isGermlineCall || anyForced(evidence)
              )
            ),
            samples
          )

        val somaticCallsOrForced =
          calls.filter(
            _.singleAlleleEvidences.exists(
              evidence ⇒ evidence.isSomaticCall || anyForced(evidence)
            )
          )

        writeSome(path("somatic.all_samples"), somaticCallsOrForced, samples)

        samples.foreach {
          sample ⇒
            if (!onlySomatic) {
              writeSome(
                path("all.%s.%s.%s".format(
                  sample.name, sample.tissueType.toString, sample.analyte.toString)),
                calls,
                Vector(sample)
              )
            }

            writeSome(
              path(
                "somatic.%s.%s.%s".format(
                  sample.name,
                  sample.tissueType.toString,
                  sample.analyte.toString
                )
              ),
              somaticCallsOrForced,
              Vector(sample)
            )
        }

        writeSome(
          path("all.tumor_pooled_dna"),
          somaticCallsOrForced,
          Vector(),
          includePooledTumor = Some(true)
        )
    }
  }
}
