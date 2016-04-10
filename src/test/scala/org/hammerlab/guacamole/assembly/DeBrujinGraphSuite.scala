package org.hammerlab.guacamole.assembly

import org.hammerlab.guacamole.Bases
import org.hammerlab.guacamole.util.TestUtil.Implicits._
import org.hammerlab.guacamole.util.{GuacFunSuite, TestUtil}

class DeBruijnGraphSuite extends GuacFunSuite {

  test("DeBruijnGraph.mergeKmers") {
    val kmers = Seq("TTTC", "TTCC", "TCCC", "CCCC").map(Bases.stringToBases)
    val longerKmer = DeBruijnGraph.mergeOverlappingSequences(kmers, 4)

    longerKmer.length should be(7)
    TestUtil.assertBases(longerKmer, "TTTCCCC")
  }

  test("build graph") {

    val sequence = Bases.stringToBases("TCATCTCAAAAGAGATCGA")
    val graph = DeBruijnGraph(Seq(sequence), kmerSize = 8)

    graph.kmerCounts === Map("TCATCTCA" -> 1, "CATCTCAA" -> 1, "GAGATCGA" -> 1)
  }

  test("build graph test prefix/suffix") {

    val sequence = "TCATCTCAAAAGAGATCGA"
    val graph = DeBruijnGraph(Seq(sequence), kmerSize = 8)

    TestUtil.assertBases(graph.kmerPrefix("TCATCTCA"), "TCATCTC")
    TestUtil.assertBases(graph.kmerPrefix("CATCTCAA"), "CATCTCA")
    TestUtil.assertBases(graph.kmerPrefix("GAGATCGA"), "GAGATCG")

    TestUtil.assertBases(graph.kmerSuffix("TCATCTCA"), "CATCTCA")
    TestUtil.assertBases(graph.kmerSuffix("CATCTCAA"), "ATCTCAA")
    TestUtil.assertBases(graph.kmerSuffix("GAGATCGA"), "AGATCGA")

  }

  test("build graph with short kmers and correct counts") {

    val sequence = Bases.stringToBases("TCATCTTAAAAGACATAAA")
    val graph = DeBruijnGraph(Seq(sequence), kmerSize = 3)

    graph.kmerCounts === Map("TCA" -> 1, "CAT" -> 2, "AAA" -> 3)
  }

  test("build graph with short kmers and correct children/parents") {

    val sequence = "TCATCTTAAAAGACATAAA"
    val graph = DeBruijnGraph(Seq(sequence), kmerSize = 3)

    val tcaChildren = graph.children("TCA")
    TestUtil.assertBases(graph.kmerSuffix("TCA"), "CA")
    tcaChildren.length should be(1) // CA is the suffix and CAT is the only kmer
    TestUtil.assertBases(tcaChildren(0), "CAT")

    val tcaParents = graph.parents("TCA")
    TestUtil.assertBases(graph.kmerPrefix("TCA"), "TC")
    tcaParents.length should be(1) // TC is the prefix and ATC is the only kmer
    TestUtil.assertBases(tcaParents(0), "ATC")

    val catParents = graph.parents("CAT")
    TestUtil.assertBases(graph.kmerPrefix("CAT"), "CA")
    catParents.length should be(2) // CA is the prefix, TCA and ACA are parents
    TestUtil.assertBases(catParents(0), "ACA")
    TestUtil.assertBases(catParents(1), "TCA")

    val catChildren = graph.children("CAT")
    TestUtil.assertBases(graph.kmerSuffix("CAT"), "AT")
    catChildren.length should be(2) // CA is the suffix, ATC and ATA are children
    TestUtil.assertBases(catChildren(0), "ATA")
    TestUtil.assertBases(catChildren(1), "ATC")
  }

  test("build graph with all unique kmers") {
    val sequence = Bases.stringToBases("AAATCCCTTTTA")
    val kmerSize = 4
    val graph = DeBruijnGraph(Seq(sequence), kmerSize)
    graph.kmerCounts.keys.size should be(sequence.length - kmerSize + 1)

    graph.kmerCounts.foreach(_._2 should be(1))

  }

  test("find forward unique path; full graph") {

    val sequence = Bases.stringToBases("AAATCCCTGGGT")
    val graph = DeBruijnGraph(Seq(sequence), kmerSize = 4)

    val firstKmer = "AAAT"

    val mergeableForward = graph.mergeForward(firstKmer)
    mergeableForward.size should be(9)

    DeBruijnGraph.mergeOverlappingSequences(mergeableForward, 4) should be(sequence)
  }

  test("find backward unique path; full graph") {

    val sequence = "AAATCCCTGGGT"
    val graph = DeBruijnGraph(Seq(Bases.stringToBases(sequence)), kmerSize = 4)

    val lastKmer = "GGGT"

    val mergeableReverse = graph.mergeBackward(lastKmer)
    mergeableReverse.size should be(9)

    val mergedReference: Seq[Byte] = DeBruijnGraph.mergeOverlappingSequences(mergeableReverse, 4)

    TestUtil.assertBases(mergedReference, sequence)
  }

  test("find forward unique path; with bubble at end") {

    val sequence = "AAATCCCTGGGT"
    val variantSequence = "AAATCCCTGGAT"
    val graph = DeBruijnGraph(
      Seq(
        Bases.stringToBases(sequence),
        Bases.stringToBases(variantSequence)
      ), kmerSize = 4)

    val firstKmer = "AAAT"

    val mergeableForward = graph.mergeForward(firstKmer)
    mergeableForward.size should be(7)
    TestUtil.assertBases(DeBruijnGraph.mergeOverlappingSequences(mergeableForward, 4), "AAATCCCTGG")
  }

  test("find forward unique path; with bubble in middle") {

    val sequence = "AAATCCCTGGGT"
    val variantSequence = "AAATCGCTGGGT"
    val graph = DeBruijnGraph(
      Seq(
        Bases.stringToBases(sequence),
        Bases.stringToBases(variantSequence)
      ), kmerSize = 4)

    val firstKmer = "AAAT"

    val mergeableForward = graph.mergeForward(firstKmer)
    mergeableForward.size should be(2)
    TestUtil.assertBases(DeBruijnGraph.mergeOverlappingSequences(mergeableForward, 4), "AAATC")
  }

  test("find forward unique path; with bubble in first kmer") {

    val sequence = "AAATCCCTGGGT"
    val variantSequence = "ACATCCCTGGGT"
    val graph = DeBruijnGraph(Seq(sequence, variantSequence), kmerSize = 4)

    val firstKmer = "AAAT"

    val mergeableForward = graph.mergeForward(firstKmer)
    mergeableForward.size should be(2)
    TestUtil.assertBases(DeBruijnGraph.mergeOverlappingSequences(mergeableForward, 4), "AAATC")
  }

  test("find backward unique path; with bubble at end") {

    val sequence = "AAATCCCTGGGT"
    val variantSequence = "AAATCCCTGGAT"
    val graph = DeBruijnGraph(Seq(sequence, variantSequence), kmerSize = 4)

    val seq1End = "GGGT"

    val seq1mergeableReverse = graph.mergeBackward(seq1End)
    seq1mergeableReverse.size should be(2)
    TestUtil.assertBases(DeBruijnGraph.mergeOverlappingSequences(seq1mergeableReverse, 4), "TGGGT")

    val seq2End = "GGAT"
    val seq2mergeableReverse = graph.mergeBackward(seq2End)
    seq2mergeableReverse.size should be(2)
    TestUtil.assertBases(DeBruijnGraph.mergeOverlappingSequences(seq2mergeableReverse, 4), "TGGAT")

  }

  test("find backward unique path; with bubble in middle") {

    val sequence = "AAATCCCTGGGT"
    val variantSequence = "AAATCGCTGGGT"
    val graph = DeBruijnGraph(Seq(sequence, variantSequence), kmerSize = 4)

    val lastKmer = "GGGT"

    val mergeableReverse = graph.mergeBackward(lastKmer)
    mergeableReverse.size should be(3)
    TestUtil.assertBases(DeBruijnGraph.mergeOverlappingSequences(mergeableReverse, 4), "CTGGGT")
  }

  test("test merge nodes; full graph") {

    val sequence = "AAATCCCTGGGT"
    val kmerSize = 4
    val graph = DeBruijnGraph(Seq(sequence), kmerSize)

    graph.kmerCounts.keys.size should be(9)

    graph.mergeNodes()
    graph.kmerCounts.keys.size should be(1)
    TestUtil.assertBases(graph.kmerCounts.keys.head, "AAATCCCTGGGT")

  }

  test("test merge nodes; with variant") {

    val sequence = "AAATCCCTGGGT"
    val variantSequence = "AAATCCCTGGAT"
    val kmerSize = 4
    val graph = DeBruijnGraph(Seq(sequence, variantSequence), kmerSize)

    graph.kmerCounts.keys.size should be(11)

    graph.mergeNodes()
    graph.kmerCounts.keys.size should be(3)

    graph.kmerCounts === Map("AAATCCCTGG" -> 1, "TGGGT" -> 1, "TGGAT" -> 1)
  }

  test("merging kmers and checking mergeIndex") {
    val longRead =
      Bases.stringToBases(
        "TGCATGGTGCTGTGAGATCAGCGTGTGTGTGTGTGCAGTGCATGGTGCTGTGTGAGATCAGCATGTGTGTGTGTGCAGTGCATGGTGCTGTGAGATCAGCGTGTGTGTGCAGCGCATGGTGCTGTGTGAGA"
      )
    val kmerSize = 45
    val graph: DeBruijnGraph = DeBruijnGraph(
      Seq(longRead),
      kmerSize,
      minOccurrence = 1,
      mergeNodes = true
    )

    longRead
      .sliding(kmerSize)
      .zipWithIndex.foreach {
        case (kmer, idx) => {
          val mergedNode = graph.mergeIndex(kmer)
          mergedNode._1 should be(longRead)
          mergedNode._2 should be(idx)
        }
      }

  }

  test("find single unique path in sequence") {

    val reference =
      "GAGGATCTGCCATGGCCGGGCGAGCTGGAGGAGCGAGGAGGAGGCAGGAGGA"

    val reads =
      Seq(
        reference.substring(0, 25),
        reference.substring(5, 30),
        reference.substring(7, 32),
        reference.substring(10, 35),
        reference.substring(19, 41),
        reference.substring(22, 44),
        reference.substring(25, 47),
        reference.substring(31, 52) + "TTT"
      )

    val kmerSize = 15
    val graph: DeBruijnGraph = DeBruijnGraph(
      reads.map(Bases.stringToBases),
      kmerSize,
      minOccurrence = 1,
      mergeNodes = false
    )

    val referenceKmerSource = reference.take(kmerSize)
    val referenceKmerSink = reference.takeRight(kmerSize)
    val paths = graph.depthFirstSearch(referenceKmerSource, referenceKmerSink)

    paths.length should be(1)
    TestUtil.assertBases(DeBruijnGraph.mergeOverlappingSequences(paths(0), kmerSize), reference)

    graph.mergeNodes()
    val pathsAfterMerging = graph.depthFirstSearch(referenceKmerSource, referenceKmerSink)
    pathsAfterMerging.length should be(1)
    TestUtil.assertBases(DeBruijnGraph.mergeOverlappingSequences(pathsAfterMerging(0), kmerSize), reference)

  }

  test("find single unique path in sequence with diverging sequence") {

    val reference =
      "GAGGATCTGCCATGGCCGGGCGAGCTGGAGGAGCGAGGAGGAGGCAGGAGGA"

    val reads =
      Seq(
        reference.substring(0, 25),
        reference.substring(5, 30),
        reference.substring(7, 32),
        reference.substring(10, 35),
        reference.substring(19, 41),
        // This is an errant read with a sequence that will lead to a dead-end
        reference.substring(19, 41) + "TCGAA",
        reference.substring(22, 44),
        reference.substring(25, 47),
        reference.substring(31, 52) + "TTT"
      )

    val kmerSize = 15
    val graph: DeBruijnGraph = DeBruijnGraph(
      reads.map(Bases.stringToBases),
      kmerSize,
      minOccurrence = 1,
      mergeNodes = false
    )

    val referenceKmerSource = reference.take(kmerSize)
    val referenceKmerSink = reference.takeRight(kmerSize)
    val paths = graph.depthFirstSearch(referenceKmerSource, referenceKmerSink)

    paths.length should be(1)
    TestUtil.assertBases(DeBruijnGraph.mergeOverlappingSequences(paths(0), kmerSize), reference)

    graph.mergeNodes()
    val pathsAfterMerging = graph.depthFirstSearch(referenceKmerSource, referenceKmerSink)
    pathsAfterMerging.length should be(1)
    TestUtil.assertBases(DeBruijnGraph.mergeOverlappingSequences(pathsAfterMerging(0), kmerSize), reference)

  }

  test("find single unique path; with multiple dead end paths/splits") {

    val reference =
      "GAGGATCTGCCATGGCCGGGCGAGCTGGAGGAGCGAGGAGGAGGCAGGAGGA"

    val reads =
      Seq(
        reference.substring(0, 25),
        reference.substring(5, 30),
        reference.substring(7, 32),
        reference.substring(10, 35),
        reference.substring(19, 41),
        // This is an errant read with a sequence that will lead to a dead-end
        reference.substring(19, 41) + "TCGAA",
        // This is a second, slightly different read that will lead to a dead-end
        reference.substring(19, 41) + "TCGTA",
        reference.substring(22, 44),
        reference.substring(25, 47),
        reference.substring(31, 52) + "TTT"
      )

    val kmerSize = 15
    val graph: DeBruijnGraph = DeBruijnGraph(
      reads.map(Bases.stringToBases),
      kmerSize,
      minOccurrence = 1,
      mergeNodes = false
    )

    val referenceKmerSource = reference.take(kmerSize)
    val referenceKmerSink = reference.takeRight(kmerSize)
    val paths = graph.depthFirstSearch(referenceKmerSource, referenceKmerSink)

    paths.length should be(1)
    TestUtil.assertBases(DeBruijnGraph.mergeOverlappingSequences(paths(0), kmerSize), reference)

    graph.mergeNodes()
    val pathsAfterMerging = graph.depthFirstSearch(referenceKmerSource, referenceKmerSink)
    pathsAfterMerging.length should be(1)
    TestUtil.assertBases(DeBruijnGraph.mergeOverlappingSequences(pathsAfterMerging(0), kmerSize), reference)

  }

  sparkTest("real reads data test") {
    val kmerSize = 55

    val referenceString = "GAGGATCTGCCATGGCCGGGCGAGCTGGAGGAGGAGGAGGAGGAGGAGGAGGAGGAGGAGGAGGAAGAGGAGGAGGCTGCAGCGGCGGCGGCGGCGAACGTGGACGACGTAGTGGTCGTGGAGGAGGTGGAGGAAGAGGCGGGGCG"
    val referenceBases = Bases.stringToBases(referenceString)
    val reference = TestUtil.makeReference(Seq(("chr2", 73613005, referenceString)), contigLengths = 73613152)

    lazy val snpReads =
      TestUtil
        .loadReads(sc, "assemble-reads-set3-chr2-73613071.sam")
        .mappedReads
        .sortBy(_.start)
        .collect

    val referenceKmerSource = referenceBases.take(kmerSize)
    val referenceKmerSink = referenceBases.takeRight(kmerSize)

    val currentGraph: DeBruijnGraph = DeBruijnGraph(
      snpReads.map(_.sequence),
      kmerSize,
      minOccurrence = 3,
      mergeNodes = false
    )

    val paths = currentGraph.depthFirstSearch(
      referenceKmerSource,
      referenceKmerSink
    )

    paths.length should be(1)

  }
}
