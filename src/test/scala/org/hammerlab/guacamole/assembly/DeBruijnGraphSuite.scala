package org.hammerlab.guacamole.assembly

import org.hammerlab.genomics.bases.Bases
import org.hammerlab.genomics.reads.ReadsUtil
import org.hammerlab.genomics.readsets.rdd.ReadsRDDUtil
import org.hammerlab.genomics.reference.Locus
import org.hammerlab.guacamole.assembly.DeBruijnGraph.{ Kmer, Sequence, mergeOverlappingSequences }
import org.hammerlab.guacamole.reference.ReferenceUtil
import org.hammerlab.guacamole.util.GuacFunSuite
import org.hammerlab.test.matchers.seqs.MapMatcher.mapMatch

import scala.collection.mutable.{ Map ⇒ MMap }

class DeBruijnGraphSuite
  extends GuacFunSuite
    with ReadsUtil
    with ReadsRDDUtil
    with ReferenceUtil {

  test("mergeKmers") {
    val kmers = Seq[Sequence]("TTTC", "TTCC", "TCCC", "CCCC")
    val longerKmer = mergeOverlappingSequences(kmers, 4)

    longerKmer.length should be(7)
    assert(longerKmer === "TTTCCCC")
  }

  def Counts(counts: (String, Int)*): MMap[Kmer, Int] = MMap(counts.map(t ⇒ Bases(t._1) → t._2): _*)

  test("build graph") {

    val read = makeRead("TCATCTCAAAAGAGATCGA")
    val graph = DeBruijnGraph(Seq(read), kmerSize = 8)

    graph.kmerCounts should mapMatch(
      Counts(
        "TCATCTCA" → 1,
        "CATCTCAA" → 1,
        "ATCTCAAA" → 1,
        "TCTCAAAA" → 1,
        "CTCAAAAG" → 1,
        "TCAAAAGA" → 1,
        "CAAAAGAG" → 1,
        "AAAAGAGA" → 1,
        "AAAGAGAT" → 1,
        "AAGAGATC" → 1,
        "AGAGATCG" → 1,
        "GAGATCGA" → 1
      )
    )
  }

  test("build graph test prefix/suffix") {

    val read = makeRead("TCATCTCAAAAGAGATCGA")
    val graph = DeBruijnGraph(Seq(read), kmerSize = 8)

    assert(graph.kmerPrefix("TCATCTCA") === "TCATCTC")
    assert(graph.kmerPrefix("CATCTCAA") === "CATCTCA")
    assert(graph.kmerPrefix("GAGATCGA") === "GAGATCG")

    assert(graph.kmerSuffix("TCATCTCA") === "CATCTCA")
    assert(graph.kmerSuffix("CATCTCAA") === "ATCTCAA")
    assert(graph.kmerSuffix("GAGATCGA") === "AGATCGA")

  }

  test("build graph with short kmers and correct counts") {

    val read = makeRead("TCATCTTAAAAGACATAAA")
    val graph = DeBruijnGraph(Seq(read), kmerSize = 3)

    graph.kmerCounts.toMap should mapMatch(
      Counts(
        "TCA" → 1,
        "CAT" → 2,
        "ATC" → 1,
        "TCT" → 1,
        "CTT" → 1,
        "TTA" → 1,
        "TAA" → 2,
        "AAA" → 3,
        "AAG" → 1,
        "AGA" → 1,
        "GAC" → 1,
        "ACA" → 1,
        "ATA" → 1
      )
    )
  }

  test("build graph and prune read based on kmer quality") {

    val highQualityRead = makeRead(
      "TCATCTTAAAAGACATAAA",
      qualityScores = Some((0 until 19).map(_ ⇒ 30))  // All base qualities are 30
    )

    val lowQualityRead = makeRead(
      "TCATCTTAAAAGACATAAA",
      qualityScores = Some((0 until 19).map(_ ⇒ 10))  // All base qualities are 10
    )

    val graph = DeBruijnGraph(
      Seq(highQualityRead, lowQualityRead),
      kmerSize = 3,
      minMeanKmerBaseQuality = 15
    )

    graph.kmerCounts should mapMatch(
      Counts(
        "TCA" → 1,
        "CAT" → 2,
        "ATC" → 1,
        "TCT" → 1,
        "CTT" → 1,
        "TTA" → 1,
        "TAA" → 2,
        "AAA" → 3,
        "AAG" → 1,
        "AGA" → 1,
        "GAC" → 1,
        "ACA" → 1,
        "ATA" → 1
      )
    )
  }

  test("build graph and prune kmers with different base quality thresholds") {

    val highQualityRead = makeRead(
      "TCATCTTAAAAGACATAAA",
      qualityScores = Some((0 until 19).map(_ ⇒ 30))  // All base qualities are 30
    )

    val mixedQualityRead = makeRead(
          "TCATCTTAA" + // Base quality 30
          "AAGACA" +    // Base quality 5
          "TAAA",       // Base quality 30
      qualityScores = Some((0 until 9).map(_ ⇒ 30) ++ (9 until 15).map(_ ⇒ 5) ++  (15 until 19).map(_ ⇒ 30))
    )

    val lowThresholdGraph =
      DeBruijnGraph(
        Seq(highQualityRead, mixedQualityRead),
        kmerSize = 3,
        minMeanKmerBaseQuality = 15
      )

    lowThresholdGraph.kmerCounts should mapMatch(
      Counts(
        "TCA" → 2,
        "CAT" → 3,
        "ATC" → 2,
        "TCT" → 2,
        "CTT" → 2,
        "TTA" → 2,
        "TAA" → 4,
        "AAA" → 5,
        "AAG" → 1,
        "AGA" → 1,
        "GAC" → 1,
        "ACA" → 1,
        "ATA" → 2
      )
    )

    val highThresholdGraph =
      DeBruijnGraph(
        Seq(highQualityRead, mixedQualityRead),
        kmerSize = 3,
        minMeanKmerBaseQuality = 25
      )

    highThresholdGraph.kmerCounts should mapMatch(
      Counts(
        "TCA" → 2,
        "CAT" → 3,
        "ATC" → 2,
        "TCT" → 2,
        "CTT" → 2,
        "TTA" → 2,
        "TAA" → 4,
        "AAA" → 4,
        "AAG" → 1,
        "AGA" → 1,
        "GAC" → 1,
        "ACA" → 1,
        "ATA" → 1
      )
    )
  }

  test("build graph with short kmers and correct children/parents") {

    val read = makeRead("TCATCTTAAAAGACATAAA")
    val graph = DeBruijnGraph(Seq(read), kmerSize = 3)

    val tcaChildren = graph.children("TCA")
    assert(graph.kmerSuffix("TCA") === "CA")
    tcaChildren.length should be(1) // CA is the suffix and CAT is the only kmer
    assert(tcaChildren(0) === "CAT")

    val tcaParents = graph.parents("TCA")
    assert(graph.kmerPrefix("TCA") === "TC")
    tcaParents.length should be(1) // TC is the prefix and ATC is the only kmer
    assert(tcaParents(0) === "ATC")

    val catParents = graph.parents("CAT")
    assert(graph.kmerPrefix("CAT") === "CA")
    catParents.length should be(2) // CA is the prefix, TCA and ACA are parents
    assert(catParents(0) === "ACA")
    assert(catParents(1) === "TCA")

    val catChildren = graph.children("CAT")
    assert(graph.kmerSuffix("CAT") === "AT")
    catChildren.length should be(2) // CA is the suffix, ATC and ATA are children
    assert(catChildren(0) === "ATA")
    assert(catChildren(1) === "ATC")
  }

  test("build graph with all unique kmers") {
    val sequence = "AAATCCCTTTTA"
    val kmerSize = 4
    val graph = DeBruijnGraph(Seq(makeRead(sequence)), kmerSize)
    graph.kmerCounts.keys.size should be(sequence.length - kmerSize + 1)

    graph.kmerCounts.foreach(_._2 should be(1))
  }

  test("find forward unique path; full graph") {

    val sequence = "AAATCCCTGGGT"
    val graph = DeBruijnGraph(Seq(makeRead(sequence)), kmerSize = 4)

    val firstKmer = "AAAT"

    val mergeableForward = graph.mergeForward(firstKmer)
    mergeableForward.size should be(9)

    assert(mergeOverlappingSequences(mergeableForward, 4) === sequence)
  }

  test("find backward unique path; full graph") {

    val sequence = "AAATCCCTGGGT"
    val graph = DeBruijnGraph(Seq(makeRead(sequence)), kmerSize = 4)

    val lastKmer = "GGGT"

    val mergeableReverse = graph.mergeBackward(lastKmer)
    mergeableReverse.size should be(9)

    val mergedReference: Sequence = mergeOverlappingSequences(mergeableReverse, 4)

    assert(mergedReference === sequence)
  }

  test("find forward unique path; with bubble at end") {

    val sequence = "AAATCCCTGGGT"
    val variantSequence = "AAATCCCTGGAT"
    val graph = DeBruijnGraph(
      Seq(
        makeRead(sequence),
        makeRead(variantSequence)
      ), kmerSize = 4)

    val firstKmer = "AAAT"

    val mergeableForward = graph.mergeForward(firstKmer)
    mergeableForward.size should be(7)
    assert(mergeOverlappingSequences(mergeableForward, 4) === "AAATCCCTGG")
  }

  test("find forward unique path; with bubble in middle") {

    val sequence = "AAATCCCTGGGT"
    val variantSequence = "AAATCGCTGGGT"
    val graph = DeBruijnGraph(
      Seq(
        makeRead(sequence),
        makeRead(variantSequence)
      ), kmerSize = 4)

    val firstKmer = "AAAT"

    val mergeableForward = graph.mergeForward(firstKmer)
    mergeableForward.size should be(2)
    assert(mergeOverlappingSequences(mergeableForward, 4) === "AAATC")
  }

  test("find forward unique path; with bubble in first kmer") {

    val read = makeRead("AAATCCCTGGGT")
    val variantRead = makeRead("ACATCCCTGGGT")
    val graph = DeBruijnGraph(Seq(read, variantRead), kmerSize = 4)

    val firstKmer = "AAAT"

    val mergeableForward = graph.mergeForward(firstKmer)
    mergeableForward.size should be(2)
    assert(mergeOverlappingSequences(mergeableForward, 4) === "AAATC")
  }

  test("find backward unique path; with bubble at end") {

    val read = makeRead("AAATCCCTGGGT")
    val variantRead = makeRead("AAATCCCTGGAT")
    val graph = DeBruijnGraph(Seq(read, variantRead), kmerSize = 4)

    val seq1End = "GGGT"

    val seq1mergeableReverse = graph.mergeBackward(seq1End)
    seq1mergeableReverse.size should be(2)
    assert(mergeOverlappingSequences(seq1mergeableReverse, 4) === "TGGGT")

    val seq2End = "GGAT"
    val seq2mergeableReverse = graph.mergeBackward(seq2End)
    seq2mergeableReverse.size should be(2)
    assert(mergeOverlappingSequences(seq2mergeableReverse, 4) === "TGGAT")

  }

  test("find backward unique path; with bubble in middle") {

    val read = makeRead("AAATCCCTGGGT")
    val variantRead = makeRead("AAATCGCTGGGT")
    val graph = DeBruijnGraph(Seq(read, variantRead), kmerSize = 4)

    val lastKmer = "GGGT"

    val mergeableReverse = graph.mergeBackward(lastKmer)
    mergeableReverse.size should be(3)
    assert(mergeOverlappingSequences(mergeableReverse, 4) === "CTGGGT")
  }

  test("test merge nodes; full graph") {

    val read = makeRead("AAATCCCTGGGT")
    val kmerSize = 4
    val graph = DeBruijnGraph(Seq(read), kmerSize)

    graph.kmerCounts.keys.size should be(9)

    graph.mergeNodes()
    graph.kmerCounts.keys.size should be(1)
    assert(graph.kmerCounts.keys.head === "AAATCCCTGGGT")
  }

  test("test merge nodes; with variant") {

    val read = makeRead("AAATCCCTGGGT")
    val variantRead = makeRead("AAATCCCTGGAT")
    val kmerSize = 4
    val graph = DeBruijnGraph(Seq(read, variantRead), kmerSize)

    graph.kmerCounts.keys.size should be(11)

    graph.mergeNodes()
    graph.kmerCounts.keys.size should be(3)

    graph.kmerCounts should mapMatch(
      Counts(
        "AAATCCCTGG" → 1,
        "TGGGT" → 1,
        "TGGAT" → 1
      )
    )
  }

  test("merging kmers and checking mergeIndex") {
    val longRead =
      makeRead(
        "TGCATGGTGCTGTGAGATCAGCGTGTGTGTGTGTGCAGTGCATGGTGCTGTGTGAGATCAGCATGTGTGTGTGTGCAGTGCATGGTGCTGTGAGATCAGCGTGTGTGTGCAGCGCATGGTGCTGTGTGAGA"
      )
    val kmerSize = 45
    val graph =
      DeBruijnGraph(
        Seq(longRead),
        kmerSize,
        minOccurrence = 1,
        mergeNodes = true
      )

    longRead
      .sequence
      .sliding(kmerSize)
      .zipWithIndex.foreach {
        case (kmer, idx) ⇒
          val mergedNode = graph.mergeIndex(kmer)
          mergedNode._1 should be(longRead.sequence)
          mergedNode._2 should be(idx)
    }
  }

  test("find single unique path in sequence") {

    val reference =
      Bases("GAGGATCTGCCATGGCCGGGCGAGCTGGAGGAGCGAGGAGGAGGCAGGAGGA")

    val reads =
      Seq(
        reference.slice(0, 25),
        reference.slice(5, 30),
        reference.slice(7, 32),
        reference.slice(10, 35),
        reference.slice(19, 41),
        reference.slice(22, 44),
        reference.slice(25, 47),
        reference.slice(31, 52) + "TTT"
      )

    val kmerSize = 15
    val graph =
      DeBruijnGraph(
        reads.map(makeRead(_)),
        kmerSize,
        minOccurrence = 1,
        mergeNodes = false
      )

    val source = reference.take(kmerSize)
    val sink = reference.takeRight(kmerSize)
    val paths = graph.depthFirstSearch(source, sink)

    paths.length should be(1)
    assert(mergeOverlappingSequences(paths(0), kmerSize) === reference)

    graph.mergeNodes()
    val pathsAfterMerging = graph.depthFirstSearch(source, sink)
    pathsAfterMerging.length should be(1)
    assert(mergeOverlappingSequences(pathsAfterMerging(0), kmerSize) === reference)
  }

  test("find single unique path in sequence with diverging sequence") {

    val reference =
      Bases("GAGGATCTGCCATGGCCGGGCGAGCTGGAGGAGCGAGGAGGAGGCAGGAGGA")

    val reads =
      Seq(
        reference.slice(0, 25),
        reference.slice(5, 30),
        reference.slice(7, 32),
        reference.slice(10, 35),
        reference.slice(19, 41),
        // This is an errant read with a sequence that will lead to a dead-end
        reference.slice(19, 41) + "TCGAA",
        reference.slice(22, 44),
        reference.slice(25, 47),
        reference.slice(31, 52) + "TTT"
      )

    val kmerSize = 15
    val graph =
      DeBruijnGraph(
        reads.map(makeRead(_)),
        kmerSize,
        minOccurrence = 1,
        mergeNodes = false
      )

    val source = reference.take(kmerSize)
    val sink = reference.takeRight(kmerSize)
    val paths = graph.depthFirstSearch(source, sink)

    paths.length should be(1)
    assert(mergeOverlappingSequences(paths(0), kmerSize) === reference)

    graph.mergeNodes()
    val pathsAfterMerging = graph.depthFirstSearch(source, sink)
    pathsAfterMerging.length should be(1)
    assert(mergeOverlappingSequences(pathsAfterMerging(0), kmerSize) === reference)
  }

  test("find single unique path; with multiple dead end paths/splits") {

    val reference =
     Bases("GAGGATCTGCCATGGCCGGGCGAGCTGGAGGAGCGAGGAGGAGGCAGGAGGA")

    val reads =
      Seq(
        reference.slice(0, 25),
        reference.slice(5, 30),
        reference.slice(7, 32),
        reference.slice(10, 35),
        reference.slice(19, 41),
        // This is an errant read with a sequence that will lead to a dead-end
        reference.slice(19, 41) + "TCGAA",
        // This is a second, slightly different read that will lead to a dead-end
        reference.slice(19, 41) + "TCGTA",
        reference.slice(22, 44),
        reference.slice(25, 47),
        reference.slice(31, 52) + "TTT"
      )

    val kmerSize = 15
    val graph =
      DeBruijnGraph(
        reads.map(makeRead(_)),
        kmerSize,
        minOccurrence = 1,
        mergeNodes = false
      )

    val source = reference.take(kmerSize)
    val sink = reference.takeRight(kmerSize)
    val paths = graph.depthFirstSearch(source, sink)

    paths.length should be(1)
    assert(mergeOverlappingSequences(paths(0), kmerSize) === reference)

    graph.mergeNodes()
    val pathsAfterMerging = graph.depthFirstSearch(source, sink)
    pathsAfterMerging.length should be(1)
    assert(mergeOverlappingSequences(pathsAfterMerging(0), kmerSize) === reference)
  }

  // Used when sorting by MappedRead.start below.
  register(classOf[Array[Locus]])

  test("real reads data test") {
    val kmerSize = 55

    val reference =
      Bases(
        "GAGGATCTGCCATGGCCGGGCGAGCTGGAGGAGGAGGAGGAGGAGGAGGA" +
        "GGAGGAGGAGGAGGAAGAGGAGGAGGCTGCAGCGGCGGCGGCGGCGAACG" +
        "TGGACGACGTAGTGGTCGTGGAGGAGGTGGAGGAAGAGGCGGGGCG"
      )

    lazy val snpReads =
      loadReadsRDD(sc, "assemble-reads-set3-chr2-73613071.sam")
        .mappedReads
        .sortBy(_.start)
        .collect

    val source = reference.take(kmerSize)
    val sink = reference.takeRight(kmerSize)

    val currentGraph =
      DeBruijnGraph(
        snpReads,
        kmerSize,
        minOccurrence = 3,
        mergeNodes = false
      )

    val paths =
      currentGraph.depthFirstSearch(
        source,
        sink
      )

    paths.length should be(1)
  }
}
