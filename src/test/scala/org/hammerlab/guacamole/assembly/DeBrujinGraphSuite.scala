package org.hammerlab.guacamole.assembly

import org.hammerlab.guacamole.reads.{Read, UnmappedRead}
import org.hammerlab.guacamole.util.TestUtil.Implicits._
import org.hammerlab.guacamole.util.{AssertBases, Bases, GuacFunSuite, TestUtil}
import org.hammerlab.guacamole.util.TestUtil._

class DeBruijnGraphSuite extends GuacFunSuite {

  def read(seq: String): Read = {
    new UnmappedRead("read", seq, "#" * seq.length, isDuplicate = false, sampleName = "sample", failedVendorQualityChecks = false, isPaired =false)
  }

  test("DeBruijnGraph.mergeKmers") {
    val kmers = Seq("TTTC", "TTCC", "TCCC", "CCCC").map(Bases.stringToBases)
    val longerKmer = DeBruijnGraph.mergeOverlappingSequences(kmers, 4)

    longerKmer.length should be(7)
    AssertBases(longerKmer, "TTTCCCC")
  }

  test("build graph") {

    val sequence = makeRead(Bases.stringToBases("TCATCTCAAAAGAGATCGA"))
    val graph = DeBruijnGraph(Seq(sequence), kmerSize = 8)

    graph.kmerCounts === Map("TCATCTCA" -> 1, "CATCTCAA" -> 1, "GAGATCGA" -> 1)
  }

  test("build graph test prefix/suffix") {

    val sequence = makeRead("TCATCTCAAAAGAGATCGA")
    val graph = DeBruijnGraph(Seq(sequence), kmerSize = 8)

    AssertBases(graph.kmerPrefix("TCATCTCA"), "TCATCTC")
    AssertBases(graph.kmerPrefix("CATCTCAA"), "CATCTCA")
    AssertBases(graph.kmerPrefix("GAGATCGA"), "GAGATCG")

    AssertBases(graph.kmerSuffix("TCATCTCA"), "CATCTCA")
    AssertBases(graph.kmerSuffix("CATCTCAA"), "ATCTCAA")
    AssertBases(graph.kmerSuffix("GAGATCGA"), "AGATCGA")

  }

  test("build graph with short kmers and correct counts") {

    val sequence = makeRead("TCATCTTAAAAGACATAAA")
    val graph = DeBruijnGraph(Seq(sequence), kmerSize = 3)

    graph.kmerCounts === Map("TCA" -> 1, "CAT" -> 2, "AAA" -> 3)
  }

  test("build graph with short kmers and correct children/parents") {

    val sequence = makeRead("TCATCTTAAAAGACATAAA")
    val graph = DeBruijnGraph(Seq(sequence), kmerSize = 3)

    val tcaChildren = graph.children("TCA")
    AssertBases(graph.kmerSuffix("TCA"), "CA")
    tcaChildren.length should be(1) // CA is the suffix and CAT is the only kmer
    AssertBases(tcaChildren(0), "CAT")

    val tcaParents = graph.parents("TCA")
    AssertBases(graph.kmerPrefix("TCA"), "TC")
    tcaParents.length should be(1) // TC is the prefix and ATC is the only kmer
    AssertBases(tcaParents(0), "ATC")

    val catParents = graph.parents("CAT")
    AssertBases(graph.kmerPrefix("CAT"), "CA")
    catParents.length should be(2) // CA is the prefix, TCA and ACA are parents
    AssertBases(catParents(0), "ACA")
    AssertBases(catParents(1), "TCA")

    val catChildren = graph.children("CAT")
    AssertBases(graph.kmerSuffix("CAT"), "AT")
    catChildren.length should be(2) // CA is the suffix, ATC and ATA are children
    AssertBases(catChildren(0), "ATA")
    AssertBases(catChildren(1), "ATC")
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

    DeBruijnGraph.mergeOverlappingSequences(mergeableForward, 4) should be(Bases.stringToBases(sequence))
  }

  test("find backward unique path; full graph") {

    val sequence = "AAATCCCTGGGT"
    val graph = DeBruijnGraph(Seq(makeRead(sequence)), kmerSize = 4)

    val lastKmer = "GGGT"

    val mergeableReverse = graph.mergeBackward(lastKmer)
    mergeableReverse.size should be(9)

    val mergedReference: Seq[Byte] = DeBruijnGraph.mergeOverlappingSequences(mergeableReverse, 4)

    AssertBases(mergedReference, sequence)
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
    AssertBases(DeBruijnGraph.mergeOverlappingSequences(mergeableForward, 4), "AAATCCCTGG")
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
    AssertBases(DeBruijnGraph.mergeOverlappingSequences(mergeableForward, 4), "AAATC")
  }

  test("find forward unique path; with bubble in first kmer") {

    val sequence = makeRead("AAATCCCTGGGT")
    val variantSequence = makeRead("ACATCCCTGGGT")
    val graph = DeBruijnGraph(Seq(sequence, variantSequence), kmerSize = 4)

    val firstKmer = "AAAT"

    val mergeableForward = graph.mergeForward(firstKmer)
    mergeableForward.size should be(2)
    AssertBases(DeBruijnGraph.mergeOverlappingSequences(mergeableForward, 4), "AAATC")
  }

  test("find backward unique path; with bubble at end") {

    val sequence = makeRead("AAATCCCTGGGT")
    val variantSequence = makeRead("AAATCCCTGGAT")
    val graph = DeBruijnGraph(Seq(sequence, variantSequence), kmerSize = 4)

    val seq1End = "GGGT"

    val seq1mergeableReverse = graph.mergeBackward(seq1End)
    seq1mergeableReverse.size should be(2)
    AssertBases(DeBruijnGraph.mergeOverlappingSequences(seq1mergeableReverse, 4), "TGGGT")

    val seq2End = "GGAT"
    val seq2mergeableReverse = graph.mergeBackward(seq2End)
    seq2mergeableReverse.size should be(2)
    AssertBases(DeBruijnGraph.mergeOverlappingSequences(seq2mergeableReverse, 4), "TGGAT")

  }

  test("find backward unique path; with bubble in middle") {

    val sequence = makeRead("AAATCCCTGGGT")
    val variantSequence = makeRead("AAATCGCTGGGT")
    val graph = DeBruijnGraph(Seq(sequence, variantSequence), kmerSize = 4)

    val lastKmer = "GGGT"

    val mergeableReverse = graph.mergeBackward(lastKmer)
    mergeableReverse.size should be(3)
    AssertBases(DeBruijnGraph.mergeOverlappingSequences(mergeableReverse, 4), "CTGGGT")
  }

  test("test merge nodes; full graph") {

    val sequence = makeRead("AAATCCCTGGGT")
    val kmerSize = 4
    val graph = DeBruijnGraph(Seq(sequence), kmerSize)

    graph.kmerCounts.keys.size should be(9)

    graph.mergeNodes()
    graph.kmerCounts.keys.size should be(1)
    AssertBases(graph.kmerCounts.keys.head, "AAATCCCTGGGT")

  }

  test("test merge nodes; with variant") {

    val sequence = makeRead("AAATCCCTGGGT")
    val variantSequence = makeRead("AAATCCCTGGAT")
    val kmerSize = 4
    val graph = DeBruijnGraph(Seq(sequence, variantSequence), kmerSize)

    graph.kmerCounts.keys.size should be(11)

    graph.mergeNodes()
    graph.kmerCounts.keys.size should be(3)

    graph.kmerCounts === Map("AAATCCCTGG" -> 1, "TGGGT" -> 1, "TGGAT" -> 1)
  }

  test("merging kmers and checking mergeIndex") {
    val longRead =
      makeRead(
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
      .sequence
      .sliding(kmerSize)
      .zipWithIndex.foreach {
        case (kmer, idx) => {
          val mergedNode = graph.mergeIndex(kmer)
          mergedNode._1 should be(longRead.sequence)
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
      reads.map(makeRead(_)),
      kmerSize,
      minOccurrence = 1,
      mergeNodes = false
    )

    val referenceKmerSource = reference.take(kmerSize)
    val referenceKmerSink = reference.takeRight(kmerSize)
    val paths = graph.depthFirstSearch(referenceKmerSource, referenceKmerSink)

    paths.length should be(1)
    AssertBases(DeBruijnGraph.mergeOverlappingSequences(paths(0), kmerSize), reference)

    graph.mergeNodes()
    val pathsAfterMerging = graph.depthFirstSearch(referenceKmerSource, referenceKmerSink)
    pathsAfterMerging.length should be(1)
    AssertBases(DeBruijnGraph.mergeOverlappingSequences(pathsAfterMerging(0), kmerSize), reference)

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
      reads.map(makeRead(_)),
      kmerSize,
      minOccurrence = 1,
      mergeNodes = false
    )

    val referenceKmerSource = reference.take(kmerSize)
    val referenceKmerSink = reference.takeRight(kmerSize)
    val paths = graph.depthFirstSearch(referenceKmerSource, referenceKmerSink)

    paths.length should be(1)
    AssertBases(DeBruijnGraph.mergeOverlappingSequences(paths(0), kmerSize), reference)

    graph.mergeNodes()
    val pathsAfterMerging = graph.depthFirstSearch(referenceKmerSource, referenceKmerSink)
    pathsAfterMerging.length should be(1)
    AssertBases(DeBruijnGraph.mergeOverlappingSequences(pathsAfterMerging(0), kmerSize), reference)

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
      reads.map(makeRead(_)),
      kmerSize,
      minOccurrence = 1,
      mergeNodes = false
    )

    val referenceKmerSource = reference.take(kmerSize)
    val referenceKmerSink = reference.takeRight(kmerSize)
    val paths = graph.depthFirstSearch(referenceKmerSource, referenceKmerSink)

    paths.length should be(1)
    AssertBases(DeBruijnGraph.mergeOverlappingSequences(paths(0), kmerSize), reference)

    graph.mergeNodes()
    val pathsAfterMerging = graph.depthFirstSearch(referenceKmerSource, referenceKmerSink)
    pathsAfterMerging.length should be(1)
    AssertBases(DeBruijnGraph.mergeOverlappingSequences(pathsAfterMerging(0), kmerSize), reference)

  }

  test("real reads data test") {
    val kmerSize = 55

    val referenceString = "GAGGATCTGCCATGGCCGGGCGAGCTGGAGGAGGAGGAGGAGGAGGAGGAGGAGGAGGAGGAGGAAGAGGAGGAGGCTGCAGCGGCGGCGGCGGCGAACGTGGACGACGTAGTGGTCGTGGAGGAGGTGGAGGAAGAGGCGGGGCG"
    val referenceBases = Bases.stringToBases(referenceString)
    val reference = TestUtil.makeReference(sc, Seq(("chr2", 73613005, referenceString)), contigLengths = 73613152)

    lazy val snpReads =
      TestUtil
        .loadReads(sc, "assemble-reads-set3-chr2-73613071.sam")
        .mappedReads
        .sortBy(_.start)
        .collect

    val referenceKmerSource = referenceBases.take(kmerSize)
    val referenceKmerSink = referenceBases.takeRight(kmerSize)

    val currentGraph: DeBruijnGraph = DeBruijnGraph(
      snpReads,
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
