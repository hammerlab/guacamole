package org.hammerlab.guacamole.commands

import breeze.linalg.DenseVector
import breeze.stats.{mean, median}
import htsjdk.samtools.CigarOperator
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole._
import org.hammerlab.guacamole.alignment.AffineGapPenaltyAlignment
import org.hammerlab.guacamole.assembly.DeBruijnGraph
import org.hammerlab.guacamole.distributed.LociPartitionUtils.partitionLociAccordingToArgs
import org.hammerlab.guacamole.distributed.WindowFlatMapUtils.windowFlatMapWithState
import org.hammerlab.guacamole.loci.LociMap
import org.hammerlab.guacamole.reads.{MappedRead, Read}
import org.hammerlab.guacamole.readsets.{GermlineCallerArgs, ReadSets}
import org.hammerlab.guacamole.reference.{ReferenceBroadcast, ReferenceGenome}
import org.hammerlab.guacamole.variants.{Allele, AlleleConversions, AlleleEvidence, CalledAllele}
import org.hammerlab.guacamole.windowing.SlidingWindow
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

    @Args4jOption(name = "--reference-fasta", required = false, usage = "Local path to a reference FASTA file")
    var referenceFastaPath: String = null

    @Args4jOption(name = "--min-area-vaf", required = false, usage = "Minimum variant allele frequency to investigate area")
    var minAreaVaf: Int = 5

    @Args4jOption(name = "--min-occurrence", required = false, usage = "Minimum occurrences to include a kmer ")
    var minOccurrence: Int = 3

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
    def getSequenceFromRead(read: MappedRead, startLocus: Int, endLocus: Int) = {
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
    def getConsensusKmer(reads: Seq[MappedRead],
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
     * @return A set of variants in the window
     *         TODO: This currently passes along a graph as state, but rebuilds it each time
     *         This can be updated to use the existing graph and just update it
     */
    def discoverHaplotypes(graph: Option[DeBruijnGraph],
                           currentWindow: SlidingWindow[MappedRead],
                           kmerSize: Int,
                           reference: ReferenceGenome,
                           minOccurrence: Int = 3,
                           expectedPloidy: Int = 2,
                           maxPathsToScore: Int = 16,
                           debugPrint: Boolean = false): (Option[DeBruijnGraph], Iterator[CalledAllele]) = {

      val locus = currentWindow.currentLocus
      val reads = currentWindow.currentRegions()

      val sampleName = reads.head.sampleName
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

      // Score up to the maximum number of paths, by aligning them against the reference
      // Take the best aligning `expectedPloidy` paths
      val pathAndAlignments =
        if (paths.size <= maxPathsToScore) {
          paths.map(path => {
            val mergedPath = DeBruijnGraph.mergeOverlappingSequences(path, kmerSize)
            (mergedPath, AffineGapPenaltyAlignment.align(mergedPath, currentReference))
          }).toSeq
            .sortBy(_._2.alignmentScore)
            .take(expectedPloidy)
        } else {
          log.warn(s"In window ${referenceContig}:${referenceStart}-$referenceEnd " +
            s"there were ${paths.size} paths found, all variants skipped")
          List.empty
        }

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
        pathAndAlignments.flatMap(kv => {
          val path = kv._1
          val alignment = kv._2

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

      (graph, variants.iterator)
    }

    override def run(args: Arguments, sc: SparkContext): Unit = {
      val reference = ReferenceBroadcast(args.referenceFastaPath, sc)
      val loci = Common.lociFromArguments(args)
      val (mappedReads, contigLengths) =
        ReadSets.loadMappedReads(
          args,
          sc,
          Read.InputFilters(
            overlapsLoci = Some(loci),
            mapped = true,
            nonDuplicate = true
          )
        )

      val minAlignmentQuality = args.minAlignmentQuality
      val qualityReads = mappedReads.filter(_.alignmentQuality > minAlignmentQuality)

      val lociPartitions =
        partitionLociAccordingToArgs(
          args,
          loci.result(contigLengths),
          mappedReads
        )

      val genotypes: RDD[CalledAllele] =
        discoverGenotypes(
          qualityReads,
          kmerSize = args.kmerSize,
          snvWindowRange = args.snvWindowRange,
          minOccurrence = args.minOccurrence,
          minAreaVaf = args.minAreaVaf / 100.0f,
          reference,
          lociPartitions
        )

      genotypes.persist()

      Common.progress(s"Found ${genotypes.count} variants")

      val outputGenotypes =
        genotypes.flatMap(AlleleConversions.calledAlleleToADAMGenotype)

      Common.writeVariantsFromArguments(args, outputGenotypes)
      DelayedMessages.default.print()
    }

    def discoverGenotypes(reads: RDD[MappedRead],
                          kmerSize: Int,
                          snvWindowRange: Int,
                          minOccurrence: Int,
                          minAreaVaf: Float,
                          reference: ReferenceBroadcast,
                          lociPartitions: LociMap[Long]): RDD[CalledAllele] = {

      val genotypes: RDD[CalledAllele] =
        windowFlatMapWithState[MappedRead, CalledAllele, Option[DeBruijnGraph]](
          Vector(reads),
          lociPartitions,
          skipEmpty = true,
          halfWindowSize = snvWindowRange,
          initialState = None,
          (graph, windows) => {
            val window = windows.head
            // Find the reads the overlap the center locus/ current locus
            val currentLocusReads =
              window
                .currentRegions()
                .filter(_.overlapsLocus(window.currentLocus))
            val variableReads =
              currentLocusReads
                .count(read =>
                  read.cigar.numCigarElements() > 1 || read.countOfMismatches(reference.getContig(window.referenceName)) > 0)

            // Compute the number reads with variant bases from the reads overlapping the currentLocus
            val currentLocusVAF = variableReads.toFloat / currentLocusReads.length
            if (currentLocusVAF > minAreaVaf) {
              val result = discoverHaplotypes(
                None,
                window,
                kmerSize,
                reference,
                minOccurrence
              )

              // Jump to the next region
              window.setCurrentLocus(window.currentLocus + snvWindowRange)
              (graph, result._2)
            } else {
              (graph, Iterator.empty)
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
    def discoverPathsFromReads(reads: Seq[MappedRead],
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
}
