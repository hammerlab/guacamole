package org.hammerlab.guacamole.commands

import breeze.linalg.DenseVector
import breeze.stats.{mean, median}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.alignment.AffineGapPenaltyAlignment
import org.hammerlab.guacamole.assembly.AssemblyUtils
import org.hammerlab.guacamole.distributed.LociPartitionUtils.{LociPartitioning, partitionLociAccordingToArgs}
import org.hammerlab.guacamole.distributed.WindowFlatMapUtils.windowFlatMapWithState
import org.hammerlab.guacamole.likelihood.Likelihood
import org.hammerlab.guacamole.logging.DelayedMessages
import org.hammerlab.guacamole.logging.LoggingUtils.progress
import org.hammerlab.guacamole.pileup.Pileup
import org.hammerlab.guacamole.reads.MappedRead
import org.hammerlab.guacamole.readsets.{GermlineCallerArgs, InputFilters, ReadSets}
import org.hammerlab.guacamole.reference.ReferenceBroadcast
import org.hammerlab.guacamole.variants.{Allele, AlleleConversions, AlleleEvidence, CalledAllele, VariantUtils}
import org.kohsuke.args4j.{Option => Args4jOption}

/**
 * Simple assembly based germline variant caller
 *
 * Overview:
 * - Find areas where > `min-area-vaf` %  of the reads show a variant
 * - Place the reads in the `snv-window-range`-base window into a DeBruijn graph
 *   and find paths between the start and end of the reference sequence that covers that region
 * - Align those paths to the reference sequence to discover variants
 *
 */
object GermlineAssemblyCaller {

  class Arguments extends GermlineCallerArgs {

    @Args4jOption(name = "--kmer-size", usage = "Length of kmer used for DeBrujin Graph assembly")
    var kmerSize: Int = 45

    @Args4jOption(name = "--snv-window-range", usage = "Number of bases before and after to check for additional matches or deletions")
    var snvWindowRange: Int = 20

    @Args4jOption(name = "--min-average-base-quality", usage = "Minimum average of base qualities in the read")
    var minAverageBaseQuality: Int = 20

    @Args4jOption(name = "--min-alignment-quality", usage = "Minimum alignment qualities of the read")
    var minAlignmentQuality: Int = 30

    @Args4jOption(name = "--reference-fasta", required = true, usage = "Local path to a reference FASTA file")
    var referenceFastaPath: String = null

    @Args4jOption(name = "--min-area-vaf", required = false, usage = "Minimum variant allele frequency to investigate area")
    var minAreaVaf: Int = 5

    @Args4jOption(name = "--min-occurrence", required = false, usage = "Minimum occurrences to include a kmer ")
    var minOccurrence: Int = 3

    @Args4jOption(name = "--min-likelihood", usage = "Minimum Phred-scaled likelihood. Default: 0 (off)")
    var minLikelihood: Int = 0

    @Args4jOption(name = "--shortcut-assembly", required = false, usage = "Skip assembly process in inactive regions")
    var shortcutAssembly: Boolean = false

  }

  object Caller extends SparkCommand[Arguments] {
    override val name = "germline-assembly"
    override val description = "call germline variants by assembling the surrounding region of reads"

    override def run(args: Arguments, sc: SparkContext): Unit = {
      val reference = ReferenceBroadcast(args.referenceFastaPath, sc)
      val loci = args.parseLoci()
      val (mappedReads, contigLengths) =
        ReadSets.loadMappedReads(
          args,
          sc,
          InputFilters(
            overlapsLoci = loci,
            mapped = true,
            nonDuplicate = true
          )
        )

      val minAlignmentQuality = args.minAlignmentQuality
      val qualityReads = mappedReads.filter(_.alignmentQuality > minAlignmentQuality)

      val lociPartitions = partitionLociAccordingToArgs(
        args,
        loci.result(contigLengths),
        mappedReads
      )

      val genotypes: RDD[CalledAllele] = discoverGermlineVariants(
        qualityReads,
        kmerSize = args.kmerSize,
        snvWindowRange = args.snvWindowRange,
        minOccurrence = args.minOccurrence,
        minAreaVaf = args.minAreaVaf / 100.0f,
        reference = reference,
        lociPartitions = lociPartitions,
        minPhredScaledLikelihood = args.minLikelihood,
        shortcutAssembly = args.shortcutAssembly)

      genotypes.persist()

      progress(s"Found ${genotypes.count} variants")

      val outputGenotypes =
        genotypes.flatMap(AlleleConversions.calledAlleleToADAMGenotype)

      VariantUtils.writeVariantsFromArguments(args, outputGenotypes)
      DelayedMessages.default.print()
    }

    def discoverGermlineVariants(reads: RDD[MappedRead],
                                 kmerSize: Int,
                                 snvWindowRange: Int,
                                 minOccurrence: Int,
                                 minAreaVaf: Float,
                                 reference: ReferenceBroadcast,
                                 lociPartitions: LociPartitioning,
                                 minAltReads: Int = 2,
                                 minPhredScaledLikelihood: Int = 0,
                                 shortcutAssembly: Boolean = false): RDD[CalledAllele] = {

      val genotypes: RDD[CalledAllele] =
        windowFlatMapWithState[MappedRead, CalledAllele, Option[Long]](
          Vector(reads),
          lociPartitions,
          skipEmpty = true,
          halfWindowSize = snvWindowRange,
          initialState = None,
          (lastCalledLocus, windows) => {
            val window = windows.head
            val referenceName = window.referenceName
            val locus = window.currentLocus

            val referenceStart = (locus - window.halfWindowSize).toInt
            val referenceEnd = (locus + window.halfWindowSize).toInt

            val currentReference =
              reference.getReferenceSequence(
                window.referenceName,
                referenceStart,
                referenceEnd
              )

            val referenceContig = reference.getContig(referenceName)

            val regionReads = window.currentRegions()
            // Find the reads the overlap the center locus/ current locus
            val currentLocusReads =
              regionReads
                .filter(_.overlapsLocus(window.currentLocus))

            val pileup = Pileup(currentLocusReads, referenceName, window.currentLocus, referenceContig)

            // Compute the number reads with variant bases from the reads overlapping the currentLocus
            val pileupAltReads = (pileup.depth - pileup.referenceDepth)
            if (currentLocusReads.isEmpty || pileupAltReads < minAltReads) {
              (lastCalledLocus, Iterator.empty)
            } else if (shortcutAssembly && !AssemblyUtils.isActiveRegion(currentLocusReads, referenceContig, minAreaVaf)) {
              val variants = callPileupVariant(pileup).filter(_.evidence.phredScaledLikelihood > minPhredScaledLikelihood)
              (variants.lastOption.map(_.start).orElse(lastCalledLocus), variants.iterator)
            } else {
              val paths = AssemblyUtils.discoverHaplotypes(
                window,
                kmerSize,
                reference,
                minOccurrence
              )

              if (paths.nonEmpty) {
                val sampleName = currentLocusReads.head.sampleName

                def buildVariant(variantLocus: Int,
                                 referenceBases: Array[Byte],
                                 alternateBases: Array[Byte]) = {
                  val allele = Allele(
                    referenceBases,
                    alternateBases
                  )

                  val depth = regionReads.length
                  val mappingQualities = DenseVector(regionReads.map(_.alignmentQuality.toFloat).toArray)
                  val baseQualities = DenseVector(regionReads.flatMap(_.baseQualities).map(_.toFloat).toArray)
                  CalledAllele(
                    sampleName,
                    referenceName,
                    variantLocus,
                    allele,
                    AlleleEvidence(
                      likelihood = 1,
                      readDepth = depth,
                      alleleReadDepth = depth,
                      forwardDepth = depth,
                      alleleForwardDepth = depth,
                      meanMappingQuality = mean(mappingQualities),
                      medianMappingQuality = median(mappingQualities),
                      meanBaseQuality = mean(baseQualities),
                      medianBaseQuality = median(baseQualities),
                      medianMismatchesPerRead = 0
                    )
                  )
                }

                val variants =
                  paths.flatMap(path =>
                    AssemblyUtils.buildVariantsFromPath[CalledAllele](
                      path,
                      referenceStart,
                      referenceContig,
                      path => AffineGapPenaltyAlignment.align(path, currentReference),
                      buildVariant
                    )
                  )
                    .toSet
                    .filter(variant => lastCalledLocus.forall(_ < variant.start)) // Filter variants before last called

                val lastVariantCallLocus = variants.view.map(_.start).reduceOption(_ max _).orElse(lastCalledLocus)
                // Jump to the next region
                window.setCurrentLocus(window.currentLocus + snvWindowRange - kmerSize)
                (lastVariantCallLocus, variants.iterator)
              } else {
                (lastCalledLocus, Iterator.empty)
              }
            }
          }
        )
      genotypes
    }



    /**
     * Call variants using a pileup and genotype likelihoods
     *
     * @param pileup Pileup at a given locus
     * @return Possible set of called variants
     */
    private def callPileupVariant(pileup: Pileup): Set[CalledAllele] = {
      val genotypeLikelihoods = Likelihood.likelihoodsOfAllPossibleGenotypesFromPileup(
        pileup,
        logSpace = true,
        normalize = true
      )

      // If we did not have any valid genotypes to evaluate
      // we do not return any variants
      if (genotypeLikelihoods.isEmpty) {
        Set.empty
      } else {
        val mostLikelyGenotypeAndProbability = genotypeLikelihoods.maxBy(_._2)

        val genotype = mostLikelyGenotypeAndProbability._1
        val probability = math.exp(mostLikelyGenotypeAndProbability._2)
        genotype
          .getNonReferenceAlleles
          .toSet // Collapse homozygous genotypes
          .filter(_.altBases.nonEmpty)
          .map(allele => {
            CalledAllele(
              pileup.head.read.sampleName,
              pileup.referenceName,
              pileup.locus,
              allele,
              AlleleEvidence(probability, allele, pileup))
          })
      }
    }
  }
}
