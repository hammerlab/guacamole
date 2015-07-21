package org.hammerlab.guacamole.assembly

import org.hammerlab.guacamole.Bases

import scala.collection.mutable

class DeBrujinGraph(val kmerSize: Int,
                    val kmerCounts: mutable.Map[DeBrujinGraph#Kmer, Int]) {

  type Kmer = Seq[Byte] // Sequence of length `kmerSize`
  type SubKmer = Seq[Byte] // Sequence of bases < `kmerSize`

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

  // Table to store kmers and what they have been merged into
  val mergeIndex: mutable.Map[Kmer, (Int, Kmer)] = mutable.Map.empty

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
   * @param kmer Kmer to remove
   */
  private[assembly] def removeKmer(kmer: Kmer) = {
    kmerCounts.remove(kmer)
    def removeFromTable(kmer: Kmer,
                        table: mutable.Map[Kmer, List[Kmer]],
                        keyFunc: Kmer => Kmer) = {
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
   * @param minSupport minimum of reads a kmer should appear in
   */
  private[assembly] def pruneKmers(minSupport: Int) = {
    kmerCounts
      .filter(_._2 < minSupport)
      .foreach({ case (kmer, count) => kmerCounts.remove(kmer) })
  }

  /**
   * Merge existing kmers in unique path
   */
  private[assembly] def mergeNodes(): Unit = {
    val allNodes: mutable.Set[Kmer] = mutable.HashSet[Kmer](kmerCounts.keys.toSeq: _*)
    
    while (!allNodes.isEmpty) {
      val node = allNodes.head
      val forwardUniquePath = mergeForward(node)
      val backwardUniquePath = mergeBackward(node)
      val fullMergeablePath = backwardUniquePath ++ forwardUniquePath.reverse.tail

      if (fullMergeablePath.length > 1) {
        // Remove everything in the merged path from the prefix/suffix tables
        fullMergeablePath.foreach(k => {
          allNodes.remove(k)
          removeKmer(k)
        })

        // Get full node from non-branch path
        val mergedNode = DeBrujinGraph.mergeKmers(fullMergeablePath)
        
        // Save each node that was merged
        fullMergeablePath.zipWithIndex.foreach({
          case (node, index) => mergeIndex.update(node, (index, mergedNode))
        })
        
        // Update the prefix and suffix tables
        val mergedNodePrefix = kmerPrefix(mergedNode)
        val mergedNodeSuffix = kmerSuffix(mergedNode)
        prefixTable(mergedNodePrefix) = mergedNode :: prefixTable.getOrElse(mergedNodePrefix, List.empty)
        suffixTable(mergedNodeSuffix) = mergedNode :: suffixTable.getOrElse(mergedNodeSuffix, List.empty)
        kmerCounts.put(mergedNode, 1)
      } else {
        allNodes.remove(node)
      }
    }
    
  }

  /**
   *  Searches from a node to merge node if a unique path exists between them
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
        nextFunc(current).filterNot(visited.contains)
      else 
        nextFunc(current)
    
    var next = nextNodes(current)
    var mergeable = List(kmer)

    // While in/out-degree == 1
    while (next.size == 1 && prevFunc(next.head).size == 1) {
      current = next.head
      visited += current
      mergeable = current :: mergeable
      next = nextNodes(current)
    }

    mergeable
  }

  private[assembly] def mergeForward(kmer: Kmer) = findMergeable(kmer, searchForward = true)
  private[assembly] def mergeBackward(kmer: Kmer) = findMergeable(kmer, searchForward = false)

  type Path = List[Kmer]
  type PathScore = Int

  /**
   * Find a path from source to sink in the graph
   * @param source Kmer node to begin search
   * @param sink Kmer to search for
   * @param kmerScore Function to score kmer
   * @param minPathLength Minimum number of kmers to traverse before finding the sink
   * @param maxPathLength Maximum number of kmers to traverse before finding the sink
   * @param maxPaths Maximum number of paths to find from source to sink
   * @param avoidLoops If avoiding loops, skip nodes that have already been visited
   * @return Set
   */
  def depthFirstSearch(source: Kmer,
                       sink: Kmer,
                       kmerScore: (Kmer => Int) = kmerCounts.getOrElse(_, 0),
                       minPathLength: Int = 1,
                       maxPathLength: Int = Int.MaxValue,
                       maxPaths: Int = 10,
                       avoidLoops: Boolean = true): List[(Path, PathScore)] = {

    assume(source.length == kmerSize)
    assume(sink.length == kmerSize)

    var paths = List.empty[(Path, PathScore)]

    // Add the source node to the frontier
    var frontier: mutable.Stack[Kmer] =
      if (mergeIndex.contains(source)) {
        val (pathIndex, mergedNode) = mergeIndex(source)
        mutable.Stack(mergedNode.drop(pathIndex))
      } else {
        mutable.Stack(source)
      }

    var visited: mutable.Set[Kmer] = mutable.Set.empty

    // Initialize an empty path
    var currentPath: Path = List.empty
    var pathScore: Int = 0

    // explore branches until we find the sink
    // or accumulate the maximum number of appropriate length paths
    while (frontier.nonEmpty && paths.size < maxPaths) {
      val next = frontier.pop()

      // increase the path score
      pathScore += kmerScore(next)

      // add the node on to the path
      currentPath = next :: currentPath
      visited += next

      // Check if the source node was merged into the current one
      lazy val foundMergedSink = mergeIndex.get(sink).exists(_._2 == next)
      val foundSink = (next == sink || foundMergedSink)
      if (!foundSink && currentPath.size < maxPathLength) {
        // Keep searching down tree
        val nextNodes = children(next)
        frontier ++= (if (avoidLoops) nextNodes.filterNot(visited.contains) else nextNodes)

      } else {
        //found sink or too long
        if (foundSink && currentPath.size + 1 >= minPathLength) {

          // Trim merged node if the sink is inside of it
          if (foundMergedSink) {
            val (mergedPathIdx, mergedNode) = mergeIndex(sink)
            val mergedPathEndIdx = mergedPathIdx + kmerSize
            currentPath = currentPath.head.dropRight(mergedNode.length - mergedPathEndIdx) :: currentPath.tail
          }
          // Found legitimate path to sink, save path
          paths = (currentPath, pathScore) :: paths
        } // else degenerate path, too long or too short
        currentPath = List.empty
        pathScore = 0
      }
    }
    paths
  }

  /**
   * Find all nodes that have in-degree = 0
   * @return
   */
  def roots: Iterable[Kmer] = {
    kmerCounts.keys.filter(parents(_).size == 0)
  }

  /**
   * Find all children of a given node
   * @param node Kmer to find parents of
   * @return  List of Kmers where their prefix matches this node's suffix
   */
  def children(node: Kmer): List[Kmer] = {
    prefixTable.getOrElse(kmerSuffix(node), List.empty)
  }

  /**
   * Find all parents of a given node
   * @param node Kmer to find parents of
   * @return List of Kmers where their suffix matches this nodes prefix
   */
  def parents(node: Kmer): List[Kmer] = {
    suffixTable.getOrElse(kmerPrefix(node), List.empty)
  }

}

object DeBrujinGraph {
  type Sequence = Seq[Byte]
  type Kmer = Seq[Byte]
  def apply(sequences: Seq[Sequence],
            kmerSize: Int,
            minOccurrence: Int = 1,
            mergeNodes: Boolean = false): DeBrujinGraph = {

    val kmerCounts = mutable.Map.empty[DeBrujinGraph#Kmer, Int]

    sequences.filter(Bases.allStandardBases(_))
      .foreach(
        _.sliding(kmerSize)
          .foreach(seq => {
            val count = kmerCounts.getOrElse(seq, 0)
            kmerCounts.update(seq, count + 1)
          })
      )

    val graph = new DeBrujinGraph(kmerSize, kmerCounts)
    //TODO(arahuja) Only add in kmers once they hit minOccurrence rather than post-pruning
    graph.pruneKmers(minOccurrence)

    if (mergeNodes) graph.mergeNodes()

    graph
  }

  def mergeKmers(kmers: Seq[Kmer]): Sequence = {
    val head = kmers.headOption.map(h => h.take(h.length - 1)).getOrElse(Seq.empty)
    val rest = kmers.map(_.last)
    head ++ rest
  }

  type Sequences = Seq[String]
}
