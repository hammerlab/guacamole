package org.hammerlab.guacamole.assembly

import grizzled.slf4j.Logging
import htsjdk.samtools.CigarOperator
import org.hammerlab.genomics.reference.ContigSequence
import org.hammerlab.guacamole.alignment.ReadAlignment
import org.hammerlab.guacamole.assembly.DeBruijnGraph.{ Sequence, discoverPathsFromReads, mergeOverlappingSequences }
import org.hammerlab.guacamole.reads.MappedRead
import org.hammerlab.guacamole.reference.ReferenceGenome
import org.hammerlab.guacamole.util.CigarUtils
import org.hammerlab.guacamole.variants.ReferenceVariant
import org.hammerlab.guacamole.windowing.SlidingWindow

import scala.collection.JavaConversions._

object AssemblyUtils extends Logging {

  /**
   * Heuristic to determine when a region has a complex collection of region alignments
   * This is based on reads with multiple mismatches or insertion / deletion alignments
   *
   * @param reads Set of reads in a reference region
   * @param referenceContig Reference contig for the region
   * @param minAreaVaf Minimum fraction of reads needed
   * @return True if the 'active' region
   */
  def isActiveRegion(reads: Seq[MappedRead],
                     referenceContig: ContigSequence,
                     minAreaVaf: Float): Boolean = {

    // Count reads with more than one mismatch or possible insertion/deletions
    val numReadsWithMismatches =
      reads.count(r =>
        r.countOfMismatches(referenceContig) > 1 || r.cigar.numCigarElements() > 1
      )

    numReadsWithMismatches / reads.size.toFloat > minAreaVaf
  }

  /**
   *
   * @param currentWindow   Window of reads that overlaps the current loci
   * @param kmerSize        Length kmers in the DeBruijn graph
   * @param minOccurrence   Minimum times a kmer must appear to be in the DeBruijn graph
   * @param expectedPloidy  Expected ploidy, or expected number of valid paths through the graph
   * @param maxPathsToScore Number of paths to align to the reference to score them
   * @return Collection of paths through the reads
   */
  def discoverHaplotypes(currentWindow: SlidingWindow[MappedRead],
                         kmerSize: Int,
                         reference: ReferenceGenome,
                         minOccurrence: Int,
                         minMeanKmerQuality: Int,
                         expectedPloidy: Int = 2,
                         maxPathsToScore: Int = 16,
                         debugPrint: Boolean = false): Seq[Sequence] = {

    val locus = currentWindow.currentLocus
    val reads = currentWindow.currentRegions()

    // TODO(ryan): normalize contigName reference
    val contigName = reads.head.contigName

    val start = (locus - currentWindow.halfWindowSize).toInt
    val end = (locus + currentWindow.halfWindowSize).toInt

    val currentReference: Array[Byte] =
      reference.getReferenceSequence(
        currentWindow.contigName,
        start,
        end
      )

    val paths =
      discoverPathsFromReads(
        reads,
        start,
        currentReference,
        kmerSize = kmerSize,
        minOccurrence = minOccurrence,
        maxPaths = maxPathsToScore + 1,
        minMeanKmerBaseQuality = minMeanKmerQuality,
        debugPrint
      )
      .map(mergeOverlappingSequences(_, kmerSize))
      .toVector

    // Score up to the maximum number of paths
    if (paths.isEmpty) {
      warn(s"In window $contigName:$start-$end assembly failed")
      List.empty
    } else if (paths.size <= expectedPloidy)
      paths
    else if (paths.size <= maxPathsToScore)
      (for {
        path ← paths
      } yield {
        reads
          .map(
            read ⇒
              -ReadAlignment(read.sequence, path).alignmentScore
          )
          .sum → path
      })
      .sortBy(-_._1)
      .take(expectedPloidy)
      .map(_._2)

    else {
      warn(s"In window $contigName:$start-$end " +
        s"there were ${paths.size} paths found, all variants skipped")
      Nil
    }
  }

  /**
   * Call variants from a path discovered from the reads
   *
   * @param path          Possible paths through the reads
   * @param start   Start locus on the reference contig or chromosome
   * @param referenceContig Reference sequence overlapping the reference region
   * @param alignPath Function that returns a ReadAlignment from the path
   * @param buildVariant Function to build a called variant from the (Locus, ReferenceAllele, Alternate)
   * @param allowReferenceVariant If true, output variants where the reference and alternate are the same
   * @return Possible sequence of called variants
   */
  def buildVariantsFromPath[T <: ReferenceVariant](path: Sequence,
                                                   start: Int,
                                                   referenceContig: ContigSequence,
                                                   alignPath: Sequence => ReadAlignment,
                                                   buildVariant: (Int, Array[Byte], Array[Byte]) => T,
                                                   allowReferenceVariant: Boolean = false): Seq[T] = {

    val alignment = alignPath(path)
    warn(s"Building variants from ${alignment.toCigarString} alignment")

    var referenceIndex = alignment.refStartIdx
    var pathIndex = 0

    // Find the alignment sequences using the CIGAR
    val cigarElements = alignment.toCigar.getCigarElements
    val numCigarElements = cigarElements.size

    cigarElements
      .zipWithIndex
      .flatMap {
        case (cigarElement, cigarIdx) ⇒
          val cigarOperator = cigarElement.getOperator
          val referenceLength = CigarUtils.getReferenceLength(cigarElement)
          val pathLength = CigarUtils.getReadLength(cigarElement)
          val locus = start + referenceIndex

          // Yield a resulting variant when there is a mismatch, insertion or deletion
          val possibleVariant =
            cigarOperator match {
              case CigarOperator.X =>
                val referenceAllele = referenceContig.slice(locus, locus + referenceLength)
                val alternateAllele = path.slice(pathIndex, pathIndex + pathLength)

                if (referenceAllele.nonEmpty &&
                    alternateAllele.nonEmpty &&
                    (allowReferenceVariant || !referenceAllele.sameElements(alternateAllele)))
                  Some(buildVariant(locus, referenceAllele, alternateAllele.toArray))
                else
                  None

              case (CigarOperator.I | CigarOperator.D) if cigarIdx != 0 && cigarIdx != (numCigarElements - 1) =>
                // For insertions and deletions, report the variant with the last reference base attached
                val referenceAllele = referenceContig.slice(locus - 1, locus + referenceLength)
                val alternateAllele = path.slice(pathIndex - 1, pathIndex + pathLength)

                if (referenceAllele.nonEmpty &&
                    alternateAllele.nonEmpty &&
                    (allowReferenceVariant || !referenceAllele.sameElements(alternateAllele)))
                  Some(buildVariant(locus - 1, referenceAllele, alternateAllele.toArray))
                else
                  None

              case _ => None
            }

          referenceIndex += referenceLength
          pathIndex += pathLength

          possibleVariant
      }
  }
}
