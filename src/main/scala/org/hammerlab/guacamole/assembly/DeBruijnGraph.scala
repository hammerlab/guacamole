package org.hammerlab.guacamole.assembly

import breeze.stats.mean
import htsjdk.samtools.CigarOperator
import org.hammerlab.guacamole.reads.{MappedRead, Read}
import org.hammerlab.guacamole.util.Bases

import scala.collection.mutable

class DeBruijnGraph(val kmerSize: Int,
                    val kmerCounts: mutable.Map[DeBruijnGraph#Kmer, Int]) {

  type Kmer = Seq[Byte] // Sequence of length `kmerSize`
  type SubKmer = Seq[Byte] // Sequence of bases < `kmerSize`
  type Sequence = Seq[Byte]

  // Table to store prefix to kmers that share that prefix
  val prefixTable: mutable.Map[SubKmer, List[Kmer]] =
    mutable.Map(
      kmerCounts
        .keys
        .groupBy(kmerPrefix)
        .map(kv => (kv._1, kv._2.toList))
        .toSeq: _*
    )

  // Table to store suffix to kmers that share that suffix
  val suffixTable: mutable.Map[SubKmer, List[Kmer]] =
    mutable.Map(
      kmerCounts
        .keys
        .groupBy(kmerSuffix)
        .map(kv => (kv._1, kv._2.toList))
        .toSeq: _*
    )

  //  Map from kmers to the sequence they were merged in to.
  //  The value is the merged sequence and the index in that sequence 
  // at which the "key" kmer occurs.
  val mergeIndex: mutable.Map[Kmer, (Sequence, Int)] = mutable.Map.empty

  @inline
  private[assembly] def kmerPrefix(seq: Kmer): SubKmer = {
    seq.take(kmerSize - 1)
  }

  @inline
  private[assembly] def kmerSuffix(seq: Kmer): SubKmer = {
    seq.takeRight(kmerSize - 1)
  }

  /**
   * Remove a kmer from the graph
   * Removes it from the count, prefix and suffix tables
   *
   * @param kmer Kmer to remove
   */
  private[assembly] def removeKmer(kmer: Kmer) = {
    kmerCounts.remove(kmer)
    def removeFromTable(kmer: Kmer,
                        table: mutable.Map[Kmer, List[Kmer]],
                        keyFunc: Kmer => SubKmer) = {
      val key = keyFunc(kmer)
      val otherNodes = table(key).filterNot(_ == kmer)
      if (otherNodes.nonEmpty) {
        table.update(key, otherNodes)
      } else {
        table.remove(key)
      }
    }

    removeFromTable(kmer, prefixTable, kmerPrefix)
    removeFromTable(kmer, suffixTable, kmerSuffix)

  }

  /**
   * Remove kmers that are not in at least minSupport reads
   *
   * @param minSupport minimum of reads a kmer should appear in
   */
  private[assembly] def pruneKmers(minSupport: Int) = {
    kmerCounts
      .filter(_._2 < minSupport)
      .foreach({ case (kmer, count) => removeKmer(kmer) })
  }

  /**
   * Merge existing kmers in unique path
   */
  private[assembly] def mergeNodes(): Unit = {
    val allNodes: mutable.Set[Kmer] = mutable.HashSet[Kmer](kmerCounts.keys.toSeq: _*)

    while (allNodes.nonEmpty) {
      val node = allNodes.head
      val forwardUniquePath = mergeForward(node)
      val backwardUniquePath = mergeBackward(node)
      val fullMergeablePath = backwardUniquePath ++ forwardUniquePath.tail

      if (fullMergeablePath.length > 1) {
        // Remove everything in the merged path from the prefix/suffix tables
        fullMergeablePath.foreach(k => {
          allNodes.remove(k)
          if (kmerCounts.contains(k)) removeKmer(k)
        })

        // Get full node from non-branch path
        val mergedNode = DeBruijnGraph.mergeOverlappingSequences(fullMergeablePath, kmerSize)

        // Save each node that was merged
        fullMergeablePath.zipWithIndex.foreach({
          case (nodeElement, index) => mergeIndex.update(nodeElement, (mergedNode, index))
        })

        // Update the prefix and suffix tables
        val mergedNodePrefix = kmerPrefix(mergedNode)
        val mergedNodeSuffix = kmerSuffix(mergedNode)
        prefixTable(mergedNodePrefix) = mergedNode :: prefixTable.getOrElse(mergedNodePrefix, List.empty)
        suffixTable(mergedNodeSuffix) = mergedNode :: suffixTable.getOrElse(mergedNodeSuffix, List.empty)
        kmerCounts.put(mergedNode, kmerCounts.getOrElse(mergedNode, 0) + 1)
      } else {
        allNodes.remove(node)
      }
    }

  }

  /**
   *  Searches forward or backward from a node to find those connected by a unique path
   *
   * @param kmer Kmer to search from
   * @param searchForward If true, search children of node, otherwise parents
   * @param avoidLoops If true, only explore a node once
   * @return List of kmers, that can be merged starting with `kmer`
   */
  private[assembly] def findMergeable(kmer: Kmer,
                                      searchForward: Boolean,
                                      avoidLoops: Boolean = true): Seq[Kmer] = {

    val nextFunc: Kmer => Seq[Kmer] = if (searchForward) children else parents
    val prevFunc: Kmer => Seq[Kmer] = if (searchForward) parents else children

    var current = kmer
    var visited: mutable.Set[Kmer] = mutable.Set(current)

    // Kmer next in the path
    def nextNodes(currentNode: Kmer) =
      if (avoidLoops)
        nextFunc(currentNode).filterNot(visited.contains)
      else
        nextFunc(currentNode)

    var next = nextNodes(current)
    var mergeable = List(current)

    // While in/out-degree == 1
    while (next.size == 1 && prevFunc(next.head).size == 1) {
      current = next.head
      visited += current
      mergeable = current :: mergeable
      next = nextNodes(current)
    }

    mergeable
  }

  private[assembly] def mergeForward(kmer: Kmer) = findMergeable(kmer, searchForward = true).reverse
  private[assembly] def mergeBackward(kmer: Kmer) = findMergeable(kmer, searchForward = false)

  type Path = List[Kmer]

  /**
   * Find a path from source to sink in the graph
   *
   * @param source Kmer node to begin search
   * @param sink Kmer to search for
   * @param minPathLength Minimum number of kmers to traverse before finding the sink
   * @param maxPathLength Maximum number of kmers to traverse before finding the sink
   * @param maxPaths Maximum number of paths to find from source to sink
   * @param avoidLoops If avoiding loops, skip nodes that have already been visited
   * @return Set
   */
  def depthFirstSearch(source: Kmer,
                       sink: Kmer,
                       minPathLength: Int = 1,
                       maxPathLength: Int = Int.MaxValue,
                       maxPaths: Int = 10,
                       avoidLoops: Boolean = true,
                       debugPrint: Boolean = false): List[(Path)] = {

    assume(source.length == kmerSize, s"Source kmer ${Bases.basesToString(source)} has size ${source.length} != $kmerSize")
    assume(sink.length == kmerSize, s"Sink kmer ${Bases.basesToString(sink)} has size ${sink.length} != $kmerSize")

    var paths = List.empty[(Path)]
    var visited: mutable.Set[Kmer] = mutable.Set.empty

    // Add the source node to the frontier
    val frontier: mutable.Stack[Kmer] =
      if (mergeIndex.contains(source)) {
        val (mergedNode, pathIndex) = mergeIndex(source)

        // Check if merged node contains the sink, if so shortcut the search as this is the only path
        val mergedSink = mergeIndex.get(sink)
        if (mergedSink.exists(node => node._1 == mergedNode && node._2 > pathIndex)) {
          val path: Path = List(mergedNode.slice(pathIndex, mergedSink.get._2 + kmerSize))
          return List(path)
        }
        // Add the merged node to the visited stack, so we don't loop back into it
        visited += mergedNode

        // Add the merged node to the frontier, removing any preceding bases
        mutable.Stack(mergedNode.drop(pathIndex))
      } else {
        mutable.Stack(source)
      }

    // Initialize an empty path
    var currentPath: Path = List.empty

    val nodeContainingSink = mergeIndex.get(sink)

    // Track if we branch in the path if we need to return to this point
    // Keep track of the branch and number of children
    val lastBranchIndex = mutable.Stack[Int]()
    val lastBranchChildren = mutable.Stack[Int]()
    var numBacktracks = 0

    // explore branches until we find the sink
    // or accumulate the maximum number of appropriate length paths
    while (frontier.nonEmpty && paths.size < maxPaths) {
      val next = frontier.pop()

      if (debugPrint) {
        if (currentPath.isEmpty) {
          println(Bases.basesToString(next))
        } else {
          println(" " * (currentPath.map(_.length).sum - kmerSize + 1) + Bases.basesToString(next))
        }
      }

      // add the node on to the path
      currentPath = next :: currentPath

      // Check if the source node was merged into the current one
      lazy val foundMergedSink = nodeContainingSink.exists(_._1 == next)
      val foundSink = (next == sink || foundMergedSink)
      val nextNodes = children(next)
      val filteredNextNodes = (if (avoidLoops) nextNodes.filterNot(visited.contains) else nextNodes)
      if (!foundSink && filteredNextNodes.nonEmpty && currentPath.size < maxPathLength) {

        // Track if this is a branching node
        if (filteredNextNodes.size > 1) {
          lastBranchIndex.push(currentPath.length)
          lastBranchChildren.push(filteredNextNodes.size)
        }

        // Keep searching down tree
        frontier.pushAll(filteredNextNodes)
        visited += next

      } else { // Found sink or search was cut short
        if (foundSink && currentPath.size >= minPathLength) { // Found sink with valid length

          // Trim merged node if the sink is inside of it
          if (foundMergedSink) {
            val (mergedNode, mergedPathIdx) = mergeIndex(sink)
            val mergedPathEndIdx = mergedPathIdx + kmerSize
            currentPath = currentPath.head.dropRight(mergedNode.length - mergedPathEndIdx) :: currentPath.tail
          }

          // Found legitimate path to sink, save path
          paths = currentPath.reverse :: paths
        } // else degenerate path, too long or too short

        if (lastBranchIndex.nonEmpty) {
          // Backtrack the current path to the last node with siblings or start
          val backtrackIndex = lastBranchIndex.top
          // Remove the nodes from the last branch from the visited set
          currentPath.slice(0, currentPath.length - backtrackIndex).foreach(visited.remove)

          // Remove the nodes from the current path
          currentPath = currentPath.drop(currentPath.length - backtrackIndex)
          numBacktracks += 1

          // If we've exhausted all paths from this backtrack point, next time we will back track to the previous point
          if (numBacktracks == lastBranchChildren.top - 1) {
            lastBranchIndex.pop()
            lastBranchChildren.pop()
            numBacktracks = 0
          }
        }
      }
    }

    if (debugPrint) {
      println(s"Found ${paths.size} paths")
    }

    paths
  }

  /**
   * Find all nodes that have in-degree = 0
   */
  def sources: Iterable[Kmer] = {
    kmerCounts.keys.filter(parents(_).isEmpty).map(_.take(kmerSize))
  }

  /**
   * Find all nodes that have out-degree = 0
   */
  def sinks: Iterable[Kmer] = {
    kmerCounts.keys.filter(children(_).isEmpty).map(_.takeRight(kmerSize))
  }

  /**
   * Find all children of a given node
   *
   * @param node Kmer to find parents of
   * @return  List of Kmers where their prefix matches this node's suffix
   */
  def children(node: Kmer): List[Kmer] = {
    prefixTable.getOrElse(kmerSuffix(node), List.empty)
  }

  /**
   * Find all parents of a given node
   *
   * @param node Kmer to find parents of
   * @return List of Kmers where their suffix matches this nodes prefix
   */
  def parents(node: Kmer): List[Kmer] = {
    suffixTable.getOrElse(kmerPrefix(node), List.empty)
  }

}

object DeBruijnGraph {
  type Sequence = DeBruijnGraph#Sequence
  type Kmer = DeBruijnGraph#Kmer

  def apply(reads: Seq[Read],
            kmerSize: Int,
            minOccurrence: Int = 1,
            minMeanKmerBaseQuality: Int = 0,
            mergeNodes: Boolean = false): DeBruijnGraph = {

    val kmerCounts = mutable.Map.empty[DeBruijnGraph#Kmer, Int]

    reads.foreach(
      read => {
        read.sequence
          .sliding(kmerSize)
          .zipWithIndex
          .foreach(
            { case (seq, index) =>
              val baseQualities = read.baseQualities.slice(index, index + kmerSize).map(_.toFloat)
              val meanBaseQuality = mean(baseQualities)
              if (meanBaseQuality > minMeanKmerBaseQuality) {
                val count = kmerCounts.getOrElse(seq, 0)
                kmerCounts.update(seq, count + 1)
              }
            }
          )
      }
    )

    val graph = new DeBruijnGraph(kmerSize, kmerCounts.filter(_._2 >= minOccurrence))

    if (mergeNodes) graph.mergeNodes()

    graph
  }

  /**
   * Merge sequences where we expect consecutive entries to overlap by `overlapSize` bases.
   *
   * @param sequences Set of sequences that we would like to combine into a single sequence
   * @param overlapSize The amount of the sequences we expect to overlap (The number of bases the last sequence overlaps
   *                    with the next. For a standard kmer graph, this is the length of the kmer length - 1
   *
   * @return A single merged sequence
   */
  def mergeOverlappingSequences(sequences: Seq[Sequence], overlapSize: Int): Sequence = {
    val head = sequences.headOption.getOrElse(Seq.empty)
    val rest = sequences.tail.flatMap(sequence => sequence.takeRight(sequence.length - overlapSize + 1))
    head ++ rest
  }

  /**
   * For a given set of reads identify all kmers that appear in the specified reference region
   *
   * @param reads  Set of reads to extract sequence from
   * @param startLocus Start (inclusive) locus on the reference
   * @param endLocus End (exclusive) locus on the reference
   * @param minOccurrence Minimum number of times a subsequence needs to appear to be included
   * @return List of subsequences overlapping [startLocus, endLocus) that appear at least `minOccurrence` time
   */
  private def getConsensusKmer(reads: Seq[MappedRead],
                               startLocus: Int,
                               endLocus: Int,
                               minOccurrence: Int): Iterable[Vector[Byte]] = {

    // Filter to reads that entirely cover the region.
    // Exclude reads that have any non-M Cigars (these don't have a 1-to-1 base mapping to the region).
    val sequences =
      for {
        read <- reads
        if !read.cigarElements.exists(_.getOperator != CigarOperator.M)
        if read.overlapsLocus(startLocus) && read.overlapsLocus(endLocus - 1)
        unclippedStart = read.unclippedStart.toInt
      } yield {
        read.sequence.slice(startLocus - unclippedStart, endLocus - unclippedStart)
      }

    // Filter to sequences that appear at least `minOccurrence` times
    sequences
      .groupBy(identity)
      .map(kv => (kv._1, kv._2.length))
      .filter(_._2 >= minOccurrence)
      .map(_._1.toVector)
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
                             minMeanKmerBaseQuality: Int,
                             debugPrint: Boolean = false) = {
    val referenceKmerSource = referenceSequence.take(kmerSize)
    val referenceKmerSink = referenceSequence.takeRight(kmerSize)


    val currentGraph: DeBruijnGraph = DeBruijnGraph(
      reads,
      kmerSize,
      minOccurrence,
      minMeanKmerBaseQuality,
      mergeNodes = true
    )

    currentGraph.depthFirstSearch(
      referenceKmerSource,
      referenceKmerSink,
      maxPaths = maxPaths,
      debugPrint = debugPrint
    )
  }
}
