package org.hammerlab.guacamole.commands

import breeze.linalg.DenseVector
import breeze.stats.{mean, median}
import htsjdk.samtools.CigarOperator
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.Common
import org.hammerlab.guacamole.Common.GermlineCallerArgs
import org.hammerlab.guacamole.alignment.AffineGapPenaltyAlignment
import org.hammerlab.guacamole.assembly.DeBruijnGraph
import org.hammerlab.guacamole.distributed.LociPartitionUtils.{LociPartitioning, partitionLociAccordingToArgs}
import org.hammerlab.guacamole.distributed.WindowFlatMapUtils.windowFlatMapWithState
import org.hammerlab.guacamole.likelihood.Likelihood
import org.hammerlab.guacamole.logging.DelayedMessages
import org.hammerlab.guacamole.logging.LoggingUtils.progress
import org.hammerlab.guacamole.pileup.Pileup
import org.hammerlab.guacamole.reads.{InputFilters, MappedRead}
import org.hammerlab.guacamole.reference.{ReferenceBroadcast, ReferenceGenome}
import org.hammerlab.guacamole.util.CigarUtils
import org.hammerlab.guacamole.variants.{Allele, AlleleConversions, AlleleEvidence, CalledAllele, VariantUtils}
import org.hammerlab.guacamole.windowing.SlidingWindow
import org.hammerlab.guacamole.Common
import org.kohsuke.args4j.{Option => Args4jOption}

import scala.collection.JavaConversions._

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

    /**
     * Extract a subsequence from a read sequence
     *
     * NOTE: This assumes that the read entirely covers the reference region and does not check for insertions
     * or deletion
     *
     * @param read Read to extract subsequence from
     * @param startLocus Start (inclusive) locus on the reference
     * @param endLocus End (exclusive) locus on the reference
     * @return Subsequence overlapping [startLocus, endLocus)
     */
    private def getSequenceFromRead(read: MappedRead, startLocus: Int, endLocus: Int) = {
      read.sequence.slice(startLocus - read.unclippedStart.toInt, endLocus - read.unclippedStart.toInt)
    }

    /**
     * For a given set of reads identify all kmers that appear in the specified reference region
     *
     * @param reads  Set of reads to extract sequence from
     * @param startLocus Start (inclusive) locus on the reference
     * @param endLocus End (exclusive) locus on the reference
     * @param minOccurrence Mininum number of times a subsequence needs to appear to be included
     * @return List of subsequences overlapping [startLocus, endLocus) that appear at least `minOccurrence` time
     */
    private def getConsensusKmer(reads: Seq[MappedRead],
                                 startLocus: Int,
                                 endLocus: Int,
                                 minOccurrence: Int): Iterable[Array[Byte]] = {

      // Filter to reads that entirely cover the region
      // Exclude reads that have any non-M Cigars (these don't have a 1 to 1 base mapping to the region)
      val overlapping = reads
        .filter(!_.cigarElements.exists(_.getOperator != CigarOperator.M))
        .filter(r => r.overlapsLocus(startLocus) && r.overlapsLocus(endLocus - 1))

      // Extract the sequences from each region
      val sequences = overlapping
        .map(r => getSequenceFromRead(r, startLocus, endLocus))

      // Filter to sequences that appear at least `minOccurrence` times
      sequences
        .groupBy(identity)
        .map(kv => (kv._1, kv._2.length))
        .filter(_._2 >= minOccurrence)
        .map(_._1.toArray)

    }

    /**
     *
     * @param graph An existing DeBruijn graph of the reads
     * @param currentWindow Window of reads that overlaps the current loci
     * @param kmerSize Length kmers in the DeBruijn graph
     * @param minOccurrence Minimum times a kmer must appear to be in the DeBruijn graph
     * @param expectedPloidy Expected ploidy, or expected number of valid paths through the graph
     * @param maxPathsToScore Number of paths to align to the reference to score them
     * @return Collection of paths through the reads
     */
    private def discoverHaplotypes(graph: Option[DeBruijnGraph],
                                   currentWindow: SlidingWindow[MappedRead],
                                   kmerSize: Int,
                                   reference: ReferenceGenome,
                                   minOccurrence: Int = 3,
                                   expectedPloidy: Int = 2,
                                   maxPathsToScore: Int = 16,
                                   debugPrint: Boolean = false): Seq[DeBruijnGraph#Sequence] = {

      val locus = currentWindow.currentLocus
      val reads = currentWindow.currentRegions()

      val referenceContig = reads.head.referenceContig

      val referenceStart = (locus - currentWindow.halfWindowSize).toInt
      val referenceEnd = (locus + currentWindow.halfWindowSize).toInt

      val currentReference: Array[Byte] = reference.getReferenceSequence(
        currentWindow.referenceName,
        referenceStart,
        referenceEnd
      )

      val paths = discoverPathsFromReads(
        reads,
        referenceStart,
        referenceEnd,
        currentReference,
        kmerSize = kmerSize,
        minOccurrence = minOccurrence,
        maxPaths = maxPathsToScore + 1,
        debugPrint)
        .map(DeBruijnGraph.mergeOverlappingSequences(_, kmerSize))
        .toVector

      // Score up to the maximum number of paths
      if (paths.isEmpty) {
        log.warn(s"In window ${referenceContig}:${referenceStart}-$referenceEnd assembly failed")
        List.empty
      } else if (paths.size <= expectedPloidy) {
        paths
      } else if (paths.size <= maxPathsToScore) {
        (for {
          path <- paths
        } yield {
          reads.map(
            read =>
              -AffineGapPenaltyAlignment.align(read.sequence, path).alignmentScore
          ).sum -> path
        })
          .sortBy(-_._1)
          .take(expectedPloidy)
          .map(_._2)

      } else {
        log.warn(s"In window ${referenceContig}:${referenceStart}-$referenceEnd " +
          s"there were ${paths.size} paths found, all variants skipped")
        List.empty
      }
    }

    override def run(args: Arguments, sc: SparkContext): Unit = {
      val reference = ReferenceBroadcast(args.referenceFastaPath, sc)
      val loci = Common.lociFromArguments(args)
      val readSet = Common.loadReadsFromArguments(
        args,
        sc,
        InputFilters(
          overlapsLoci = loci,
          mapped = true,
          nonDuplicate = true
        )
      )

      val minAlignmentQuality = args.minAlignmentQuality
      val qualityReads = readSet
        .mappedReads
        .filter(_.alignmentQuality > minAlignmentQuality)

      val lociPartitions = partitionLociAccordingToArgs(
        args,
        loci.result(readSet.contigLengths),
        readSet.mappedReads
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

            // Find the reads the overlap the center locus/ current locus
            val currentLocusReads =
              window
                .currentRegions()
                .filter(_.overlapsLocus(window.currentLocus))

            val pileup = Pileup(currentLocusReads, referenceName, window.currentLocus, referenceContig)

            // Compute the number reads with variant bases from the reads overlapping the currentLocus
            val pileupAltReads = (pileup.depth - pileup.referenceDepth)
            if (currentLocusReads.isEmpty || pileupAltReads < minAltReads) {
              (lastCalledLocus, Iterator.empty)
            } else if (
              shortcutAssembly &&
                {
                  val numReadsWithMismatches =
                    currentLocusReads.count(r =>
                      r.countOfMismatches(referenceContig) > 1 || r.cigar.numCigarElements() > 1
                    )
                  
                  numReadsWithMismatches / currentLocusReads.size.toFloat < minAreaVaf
                }
            ) {
              val variants = callPileupVariant(pileup).filter(_.evidence.phredScaledLikelihood > minPhredScaledLikelihood)
              (variants.lastOption.map(_.start).orElse(lastCalledLocus), variants.iterator)
            } else {
              val paths = discoverHaplotypes(
                None,
                window,
                kmerSize,
                reference,
                minOccurrence
              )

              if (paths.nonEmpty) {
                val variants =
                  buildVariantsFromPaths(
                    paths,
                    currentLocusReads.head.sampleName,
                    referenceName,
                    referenceStart = (window.currentLocus - window.halfWindowSize).toInt,
                    currentReference,
                    window.currentRegions()
                  )
                  .filter(variant => lastCalledLocus.forall(_ < variant.start)) // Filter variants before last called

                // Jump to the next region
                window.setCurrentLocus(window.currentLocus + snvWindowRange)
                (variants.lastOption.map(_.start).orElse(lastCalledLocus), variants.iterator)
              } else {
                (lastCalledLocus, Iterator.empty)
              }
            }
          }
        )
      genotypes
    }

    /**
     * Find paths through the reads given that represent the sequence covering referenceStart and referenceEnd
     *
     * @param reads Reads to use to build the graph
     * @param referenceStart Start of the reference region corresponding to the reads
     * @param referenceEnd End of the reference region corresponding to the reads
     * @param referenceSequence Reference sequence overlapping [referenceStart, referenceEnd)
     * @param kmerSize Length of kmers to use to traverse the paths
     * @param minOccurrence Minimum number of occurrences of the each kmer
     * @param maxPaths Maximum number of paths to find
     * @param debugPrint Print debug statements (default: false)
     * @return List of paths that traverse the region
     */
    private def discoverPathsFromReads(reads: Seq[MappedRead],
                                       referenceStart: Int,
                                       referenceEnd: Int,
                                       referenceSequence: Array[Byte],
                                       kmerSize: Int,
                                       minOccurrence: Int,
                                       maxPaths: Int,
                                       debugPrint: Boolean = false) = {
      val referenceKmerSource = referenceSequence.take(kmerSize)
      val referenceKmerSink = referenceSequence.takeRight(kmerSize)

      val sources: Set[Array[Byte]] = (getConsensusKmer(
        reads,
        referenceStart,
        referenceStart + kmerSize,
        minOccurrence = minOccurrence
      ) ++ Seq(referenceKmerSource)).toSet

      val sinks: Set[Array[Byte]] = (getConsensusKmer(
        reads,
        referenceEnd - kmerSize,
        referenceEnd,
        minOccurrence = minOccurrence
      ) ++ Seq(referenceKmerSink)).toSet

      val currentGraph: DeBruijnGraph = DeBruijnGraph(
        reads.map(_.sequence),
        kmerSize,
        minOccurrence,
        mergeNodes = true
      )

      for {
        source <- sources
        sink <- sinks
        path <- currentGraph.depthFirstSearch(
          source,
          sink,
          maxPaths = maxPaths,
          debugPrint = debugPrint
        )
      } yield {
        path
      }
    }
  }

  /**
   * Call variants from the paths discovered from the reads
   *
   * @param paths Sequence of possible paths through the reads
   * @param sampleName Name of the sample
   * @param referenceContig Reference contig or chromosome
   * @param referenceStart Start locus on the reference contig or chromosome
   * @param currentReference Reference sequence overlapping the reference region
   * @param reads Set of reads used to build paths
   * @return Possible sequence of called variants
   */
  private def buildVariantsFromPaths(paths: Seq[DeBruijnGraph#Sequence],
                                     sampleName: String,
                                     referenceContig: String,
                                     referenceStart: Int,
                                     currentReference: Array[Byte],
                                     reads: Seq[MappedRead]): Set[CalledAllele] = {

    // Build a variant using the current offset and read evidence
    def buildVariant(referenceOffset: Int,
                     referenceBases: Array[Byte],
                     alternateBases: Array[Byte]) = {
      val allele = Allele(
        referenceBases,
        alternateBases
      )

      val depth = reads.length
      val mappingQualities = DenseVector(reads.map(_.alignmentQuality.toFloat).toArray)
      val baseQualities = DenseVector(reads.flatMap(_.baseQualities).map(_.toFloat).toArray)
      CalledAllele(
        sampleName,
        referenceContig,
        referenceStart + referenceOffset,
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
      paths.flatMap(path => {
        val alignment = AffineGapPenaltyAlignment.align(path, currentReference)

        var referenceIndex = alignment.refStartIdx
        var pathIndex = 0

        // Find the alignment sequences using the CIGAR
        val cigarElements = alignment.toCigar.getCigarElements

        cigarElements.flatMap(cigarElement => {
          val cigarOperator = cigarElement.getOperator
          val referenceLength = CigarUtils.getReferenceLength(cigarElement)
          val pathLength = CigarUtils.getReadLength(cigarElement)

          // Yield a resulting variant when there is a mismatch, insertion or deletion
          val possibleVariant = cigarOperator match {
            case CigarOperator.X =>
              val referenceAllele = currentReference.slice(referenceIndex, referenceIndex + referenceLength)
              val alternateAllele = path.slice(pathIndex, pathIndex + pathLength)
              Some(buildVariant(referenceIndex, referenceAllele, alternateAllele.toArray))
            case (CigarOperator.I | CigarOperator.D) if referenceIndex != 0 =>
              // For insertions and deletions, report the variant with the last reference base attached
              val referenceAllele = currentReference.slice(referenceIndex - 1, referenceIndex + referenceLength)
              val alternateAllele = path.slice(pathIndex - 1, pathIndex + pathLength)
              Some(buildVariant(referenceIndex - 1, referenceAllele, alternateAllele.toArray))
            case _ => None
          }

          referenceIndex += referenceLength
          pathIndex += pathLength

          possibleVariant
        })
      })

    variants.toSet
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
