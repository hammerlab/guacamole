package org.hammerlab.guacamole.assembly

import org.hammerlab.guacamole.Bases
import org.hammerlab.guacamole.reads.MDTagUtils
import org.hammerlab.guacamole.util.TestUtil.Implicits._
import org.hammerlab.guacamole.util.{ GuacFunSuite, TestUtil }
import org.scalatest.Matchers

class DeBrujinGraphSuite extends GuacFunSuite with Matchers {

  test("DeBrujinGraph.mergeKmers") {
    val kmers = Seq("TTTC", "TTCC", "TCCC", "CCCC").map(Bases.stringToBases)
    val longerKmer = DeBrujinGraph.mergeKmers(kmers)

    longerKmer.length should be(7)
    TestUtil.assertBases(longerKmer, "TTTCCCC")
  }

  test("build graph") {

    val sequence = Bases.stringToBases("TCATCTCAAAAGAGATCGA")
    val graph = DeBrujinGraph(Seq(sequence), kmerSize = 8)

    val firstKmer = "TCATCTCA"
    val nextKmer = "CATCTCAA"
    val lastKmer = "GAGATCGA"

    assert(graph.kmerCounts.contains(firstKmer))
    assert(graph.kmerCounts.contains(nextKmer))
    assert(graph.kmerCounts.contains(lastKmer))
  }

  test("build graph test prefix/suffix") {

    val sequence = "TCATCTCAAAAGAGATCGA"
    val graph = DeBrujinGraph(Seq(sequence), kmerSize = 8)

    val firstKmer = "TCATCTCA"
    val nextKmer = "CATCTCAA"
    val lastKmer = "GAGATCGA"

    TestUtil.assertBases(graph.kmerPrefix("TCATCTCA"), "TCATCTC")
    TestUtil.assertBases(graph.kmerPrefix("CATCTCAA"), "CATCTCA")
    TestUtil.assertBases(graph.kmerPrefix("GAGATCGA"), "GAGATCG")

    TestUtil.assertBases(graph.kmerSuffix("TCATCTCA"), "CATCTCA")
    TestUtil.assertBases(graph.kmerSuffix("CATCTCAA"), "ATCTCAA")
    TestUtil.assertBases(graph.kmerSuffix("GAGATCGA"), "AGATCGA")

  }

  test("build graph with short kmers and correct counts") {

    val sequence = Bases.stringToBases("TCATCTTAAAAGACATAAA")
    val graph = DeBrujinGraph(Seq(sequence), kmerSize = 3)

    val firstKmer = "TCA"
    val nextKmer = "CAT"
    val lastKmer = "AAA"

    assert(graph.kmerCounts(firstKmer) === 1)
    assert(graph.kmerCounts(nextKmer) === 2)
    assert(graph.kmerCounts(lastKmer) === 3)
  }

  test("build graph with short kmers and correct children/parents") {

    val sequence = "TCATCTTAAAAGACATAAA"
    val graph = DeBrujinGraph(Seq(sequence), kmerSize = 3)

    val firstKmer = "TCA"
    val nextKmer = "CAT"
    val lastKmer = "AAA"

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
  }

  test("build graph with all unique kmers") {
    val sequence = Bases.stringToBases("AAATCCCTTTTA")
    val kmerSize = 4
    val graph = DeBrujinGraph(Seq(sequence), kmerSize)
    graph.kmerCounts.keys.size should be(sequence.length - kmerSize + 1)

    graph.kmerCounts.foreach(_._2 should be(1))

  }

  test("find forward unique path; full graph") {

    val sequence = Bases.stringToBases("AAATCCCTGGGT")
    val graph = DeBrujinGraph(Seq(sequence), kmerSize = 4)

    val firstKmer = "AAAT"

    val merageableForward = graph.mergeForward(firstKmer)
    merageableForward.size should be(9)

    DeBrujinGraph.mergeKmers(merageableForward.reverse) should be(sequence)
  }

  test("find backward unique path; full graph") {

    val sequence = "AAATCCCTGGGT"
    val graph = DeBrujinGraph(Seq(Bases.stringToBases(sequence)), kmerSize = 4)

    val firstKmer = "GGGT"

    val merageableReverse = graph.mergeBackward(firstKmer)
    merageableReverse.size should be(9)

    val mergedReference: Seq[Byte] = DeBrujinGraph.mergeKmers(merageableReverse)

    TestUtil.assertBases(mergedReference, sequence)
  }

  test("find forward unique path; with bubble") {

    val sequence = "AAATCCCTGGGT"
    val variantSequence = "AAATCCCTGGAT"
    val graph = DeBrujinGraph(
      Seq(
        Bases.stringToBases(sequence),
        Bases.stringToBases(variantSequence)
      ), kmerSize = 4)

    val firstKmer = "AAAT"

    val merageableForward = graph.mergeForward(firstKmer)
    merageableForward.size should be(7)
    TestUtil.assertBases(DeBrujinGraph.mergeKmers(merageableForward.reverse), "AAATCCCTGG")
  }

  test("find forward unique path; with bubble in middle") {

    val sequence = "AAATCCCTGGGT"
    val variantSequence = "AAATCGCTGGGT"
    val graph = DeBrujinGraph(
      Seq(
        Bases.stringToBases(sequence),
        Bases.stringToBases(variantSequence)
      ), kmerSize = 4)

    val firstKmer = "AAAT"

    val merageableForward = graph.mergeForward(firstKmer)
    merageableForward.size should be(2)
    TestUtil.assertBases(DeBrujinGraph.mergeKmers(merageableForward.reverse), "AAATC")
  }

  test("find forward unique path; with bubble in first kmer") {

    val sequence = "AAATCCCTGGGT"
    val variantSequence = "ACATCCCTGGGT"
    val graph = DeBrujinGraph(Seq(sequence, variantSequence), kmerSize = 4)

    val firstKmer = "AAAT"

    val merageableForward = graph.mergeForward(firstKmer)
    merageableForward.size should be(2)
    TestUtil.assertBases(DeBrujinGraph.mergeKmers(merageableForward.reverse), "AAATC")
  }

  test("find backward unique path; with bubble") {

    val sequence = "AAATCCCTGGGT"
    val variantSequence = "AAATCCCTGGAT"
    val graph = DeBrujinGraph(Seq(sequence, variantSequence), kmerSize = 4)

    val seq1End = "GGGT"

    val seq1MerageableReverse = graph.mergeBackward(seq1End)
    seq1MerageableReverse.size should be(2)
    TestUtil.assertBases(DeBrujinGraph.mergeKmers(seq1MerageableReverse), "TGGGT")

    val seq2End = "GGAT"
    val seq2MerageableReverse = graph.mergeBackward(seq2End)
    seq2MerageableReverse.size should be(2)
    TestUtil.assertBases(DeBrujinGraph.mergeKmers(seq2MerageableReverse), "TGGAT")

  }

  test("find backward unique path; with bubble in middle") {

    val sequence = "AAATCCCTGGGT"
    val variantSequence = "AAATCGCTGGGT"
    val graph = DeBrujinGraph(Seq(sequence, variantSequence), kmerSize = 4)

    val firstKmer = "AAAT"

    val merageableForward = graph.mergeForward(firstKmer)
    merageableForward.size should be(2)
    TestUtil.assertBases(DeBrujinGraph.mergeKmers(merageableForward.reverse), "AAATC")
  }

  test("test merge nodes; full graph") {

    val sequence = "AAATCCCTGGGT"
    val kmerSize = 4
    val graph = DeBrujinGraph(Seq(sequence), kmerSize)

    graph.kmerCounts.keys.size should be(9)

    graph.mergeNodes()
    graph.kmerCounts.keys.size should be(1)
    TestUtil.assertBases(graph.kmerCounts.keys.head, "AAATCCCTGGGT")

  }

  test("test merge nodes; with variant") {

    val sequence = "AAATCCCTGGGT"
    val variantSequence = "AAATCCCTGGAT"
    val kmerSize = 4
    val graph = DeBrujinGraph(Seq(sequence, variantSequence), kmerSize)

    graph.kmerCounts.keys.size should be(11)

    graph.mergeNodes()
    graph.kmerCounts.keys.size should be(3)

    assert(graph.kmerCounts.contains("AAATCCCTGG"))

    assert(graph.kmerCounts.contains("TGGGT"))
    assert(graph.kmerCounts.contains("TGGAT"))

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
    val graph: DeBrujinGraph = DeBrujinGraph(
      reads.map(Bases.stringToBases),
      kmerSize,
      minOccurrence = 1,
      mergeNodes = false
    )

    val referenceKmerSource = reference.take(kmerSize)
    val referenceKmerSink = reference.takeRight(kmerSize)
    val paths = graph.depthFirstSearch(referenceKmerSource, referenceKmerSink)

    paths.length should be(1)
    TestUtil.assertBases(DeBrujinGraph.mergeKmers(paths(0)._1.reverse), reference)
    
    graph.mergeNodes()
    val pathsAfterMerging = graph.depthFirstSearch(referenceKmerSource, referenceKmerSink)
    pathsAfterMerging.length should be(1)
    TestUtil.assertBases(DeBrujinGraph.mergeKmers(pathsAfterMerging(0)._1.reverse), reference)

  }

  sparkTest("real reads data test") {
    val kmerSize = 55

    lazy val snpReads = TestUtil.loadReads(
      sc,
      "assemble-reads-set3-chr2-73613071.sam")
      .mappedReads
      .sortBy(_.start)
      .collect

    val reference = MDTagUtils.getReference(
      snpReads,
      referenceStart = 73613005,
      referenceEnd = 73613151
    )

    val referenceKmerSource = reference.take(kmerSize)
    val referenceKmerSink = reference.takeRight(kmerSize)

    println(snpReads.length)

    val currentGraph: DeBrujinGraph = DeBrujinGraph(
      snpReads.map(read => read.sequence),
      kmerSize,
      minOccurrence = 1,
      mergeNodes = false
    )

    val paths = currentGraph.depthFirstSearch(
      referenceKmerSource,
      referenceKmerSink
    )
    
    paths.length should be(1)

  }
}
