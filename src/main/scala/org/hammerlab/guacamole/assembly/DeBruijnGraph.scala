package org.hammerlab.guacamole.assembly

import breeze.stats.mean
import org.hammerlab.genomics.bases.Bases
import org.hammerlab.genomics.reference.{ KmerLength, Locus }
import org.hammerlab.guacamole.assembly.DeBruijnGraph.{ Kmer, Path, Sequence, SubKmer, mergeOverlappingSequences }
import org.hammerlab.guacamole.reads.{ MappedRead, Read }
import org.hammerlab.guacamole.util.Bases.basesToString

import scala.collection.mutable.{ HashSet ⇒ MHashSet, Map ⇒ MMap, Set ⇒ MSet, Stack ⇒ MStack }

class DeBruijnGraph(val kmerSize: KmerLength,
                    val kmerCounts: MMap[Kmer, Int]) {

  // Table to store prefix to kmers that share that prefix
  val prefixTable: MMap[SubKmer, List[Kmer]] =
    MMap(
      kmerCounts
        .keys
        .groupBy(kmerPrefix)
        .map(kv => (kv._1, kv._2.toList))
        .toSeq: _*
    )

  // Table to store suffix to kmers that share that suffix
  val suffixTable: MMap[SubKmer, List[Kmer]] =
    MMap(
      kmerCounts
        .keys
        .groupBy(kmerSuffix)
        .mapValues(_.toList)
        .toSeq: _*
    )

  //  Map from kmers to the sequence they were merged in to.
  //  The value is the merged sequence and the index in that sequence 
  // at which the "key" kmer occurs.
  val mergeIndex: MMap[Kmer, (Sequence, Int)] = MMap.empty

  @inline
  private[assembly] def kmerPrefix(seq: Kmer): SubKmer =
    seq.take(kmerSize - 1)

  @inline
  private[assembly] def kmerSuffix(seq: Kmer): SubKmer =
    seq.takeRight(kmerSize - 1)

  /**
   * Remove a kmer from the graph
   * Removes it from the count, prefix and suffix tables
   *
   * @param kmer Kmer to remove
   */
  private[assembly] def removeKmer(kmer: Kmer) = {
    kmerCounts.remove(kmer)
    def removeFromTable(kmer: Kmer,
                        table: MMap[Kmer, List[Kmer]],
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
  private[assembly] def pruneKmers(minSupport: Int) =
    kmerCounts
      .filter { _._2 < minSupport }
      .foreach { case (kmer, _) => removeKmer(kmer) }

  /**
   * Merge existing kmers in unique path
   */
  private[assembly] def mergeNodes(): Unit = {
    val allNodes: MSet[Kmer] = MHashSet[Kmer](kmerCounts.keys.toSeq: _*)

    while (allNodes.nonEmpty) {
      val node = allNodes.head
      val forwardUniquePath = mergeForward(node)
      val backwardUniquePath = mergeBackward(node)
      val fullMergeablePath = backwardUniquePath ++ forwardUniquePath.tail

      if (fullMergeablePath.length > 1) {
        // Remove everything in the merged path from the prefix/suffix tables
        fullMergeablePath.foreach { k ⇒
          allNodes.remove(k)
          if (kmerCounts.contains(k)) removeKmer(k)
        }

        // Get full node from non-branch path
        val mergedNode = mergeOverlappingSequences(fullMergeablePath, kmerSize)

        // Save each node that was merged
        fullMergeablePath
          .zipWithIndex
          .foreach {
            case (nodeElement, index) =>
              mergeIndex.update(nodeElement, (mergedNode, index))
          }

        // Update the prefix and suffix tables
        val mergedNodePrefix = kmerPrefix(mergedNode)
        val mergedNodeSuffix = kmerSuffix(mergedNode)
        prefixTable(mergedNodePrefix) = mergedNode :: prefixTable.getOrElse(mergedNodePrefix, List.empty)
        suffixTable(mergedNodeSuffix) = mergedNode :: suffixTable.getOrElse(mergedNodeSuffix, List.empty)
        kmerCounts.put(mergedNode, kmerCounts.getOrElse(mergedNode, 0) + 1)
      } else
        allNodes.remove(node)
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
    var visited: MSet[Kmer] = MSet(current)

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
                       debugPrint: Boolean = false): List[Path] = {

    assume(source.length == kmerSize, s"Source kmer ${basesToString(source)} has size ${source.length} != $kmerSize")
    assume(sink.length == kmerSize, s"Sink kmer ${basesToString(sink)} has size ${sink.length} != $kmerSize")

    var paths = List.empty[Path]
    var visited: MSet[Kmer] = MSet.empty

    // Add the source node to the frontier
    val frontier: MStack[Kmer] =
      if (mergeIndex.contains(source)) {
        val (mergedNode, pathIndex) = mergeIndex(source)

        // Check if merged node contains the sink, if so shortcut the search as this is the only path
        val mergedSink = mergeIndex.get(sink)
        if (
          mergedSink.exists {
            case (sequence, index) ⇒
              sequence == mergedNode && index > pathIndex
          }
        ) {
          val path: Path = List(mergedNode.slice(pathIndex, mergedSink.get._2 + kmerSize))
          return List(path)
        }
        // Add the merged node to the visited stack, so we don't loop back into it
        visited += mergedNode

        // Add the merged node to the frontier, removing any preceding bases
        MStack(mergedNode.drop(pathIndex))
      } else {
        MStack(source)
      }

    // Initialize an empty path
    var currentPath: Path = List.empty

    val nodeContainingSink = mergeIndex.get(sink)

    // Track if we branch in the path if we need to return to this point
    // Keep track of the branch and number of children
    val lastBranchIndex = MStack[Int]()
    val lastBranchChildren = MStack[Int]()
    var numBacktracks = 0

    // explore branches until we find the sink
    // or accumulate the maximum number of appropriate length paths
    while (frontier.nonEmpty && paths.size < maxPaths) {
      val next = frontier.pop()

      if (debugPrint) {
        if (currentPath.isEmpty)
          println(basesToString(next))
        else
          println(" " * (currentPath.map(_.length).sum - kmerSize + 1) + basesToString(next))
      }

      // add the node on to the path
      currentPath = next :: currentPath

      // Check if the source node was merged into the current one
      lazy val foundMergedSink = nodeContainingSink.exists(_._1 == next)
      val foundSink = next == sink || foundMergedSink
      val nextNodes = children(next)
      val filteredNextNodes =
        if (avoidLoops)
          nextNodes.filterNot(visited.contains)
        else
          nextNodes

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
  def sources: Iterable[Kmer] =
    kmerCounts
      .keys
      .filter(parents(_).isEmpty)
      .map(_.take(kmerSize): Bases)

  /**
   * Find all nodes that have out-degree = 0
   */
  def sinks: Iterable[Kmer] =
    kmerCounts
      .keys
      .filter(children(_).isEmpty)
      .map(_.takeRight(kmerSize): Bases)

  /**
   * Find all children of a given node
   *
   * @param node Kmer to find parents of
   * @return  List of Kmers where their prefix matches this node's suffix
   */
  def children(node: Kmer): List[Kmer] =
    prefixTable
      .getOrElse(
        kmerSuffix(node),
        Nil
      )

  /**
   * Find all parents of a given node
   *
   * @param node Kmer to find parents of
   * @return List of Kmers where their suffix matches this nodes prefix
   */
  def parents(node: Kmer): List[Kmer] =
    suffixTable
      .getOrElse(
        kmerPrefix(node),
        Nil
      )
}

object DeBruijnGraph {
  type Kmer = Bases // Sequence of length `kmerSize`
  type SubKmer = Bases // Sequence of bases < `kmerSize`
  type Sequence = Bases
  type Path = List[Kmer]

  def apply(reads: Seq[Read],
            kmerSize: Int,
            minOccurrence: Int = 1,
            minMeanKmerBaseQuality: Int = 0,
            mergeNodes: Boolean = false): DeBruijnGraph = {

    val kmerCounts = MMap.empty[Kmer, Int]

    reads.foreach {
      read ⇒
        read
          .sequence
          .sliding(kmerSize)
          .zipWithIndex
          .foreach {
            case (seq, index) ⇒
              val baseQualities = read.baseQualities.slice(index, index + kmerSize).map(_.toFloat)
              val meanBaseQuality = mean(baseQualities)
              if (meanBaseQuality > minMeanKmerBaseQuality) {
                val count = kmerCounts.getOrElse(seq, 0)
                kmerCounts.update(seq, count + 1)
              }
          }
    }

    val graph =
      new DeBruijnGraph(
        kmerSize,
        kmerCounts.filter(_._2 >= minOccurrence)
      )

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
    val head: Bases = sequences.headOption.getOrElse(Bases.empty)
    val rest =
      sequences
        .tail
        .flatMap(_.drop(overlapSize - 1))

    head ++ rest
  }

  /**
   * Find paths through the reads given that represent the sequence covering referenceStart and referenceEnd
   *
   * @param reads Reads to use to build the graph
   * @param referenceStart Start of the reference region corresponding to the reads
   * @param referenceSequence Reference sequence overlapping [referenceStart, referenceEnd)
   * @param kmerSize Length of kmers to use to traverse the paths
   * @param minOccurrence Minimum number of occurrences of the each kmer
   * @param maxPaths Maximum number of paths to find
   * @param debugPrint Print debug statements (default: false)
   * @return List of paths that traverse the region
   */
  def discoverPathsFromReads(reads: Seq[MappedRead],
                             referenceStart: Locus,
                             referenceSequence: Bases,
                             kmerSize: KmerLength,
                             minOccurrence: Int,
                             maxPaths: Int,
                             minMeanKmerBaseQuality: Int,
                             debugPrint: Boolean = false): List[Path] = {

    val referenceKmerSource = referenceSequence.take(kmerSize)
    val referenceKmerSink = referenceSequence.takeRight(kmerSize)

    val currentGraph =
      DeBruijnGraph(
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
