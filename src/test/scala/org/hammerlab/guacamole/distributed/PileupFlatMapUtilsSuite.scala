package org.hammerlab.guacamole.distributed

import org.apache.spark.storage.BroadcastBlockId
import org.hammerlab.genomics.bases.Base.{ A, C, G, N, T }
import org.hammerlab.genomics.bases.{ Base, Bases }
import org.hammerlab.genomics.loci.set.test.LociSetUtil
import org.hammerlab.genomics.readsets.PerSample
import org.hammerlab.genomics.readsets.rdd.ReadsRDDUtil
import org.hammerlab.genomics.reference.Locus
import org.hammerlab.guacamole.distributed.PileupFlatMapUtils.{ pileupFlatMapMultipleSamples, pileupFlatMapOneSample, pileupFlatMapTwoSamples }
import org.hammerlab.guacamole.distributed.Util.pileupsToElementStrings
import org.hammerlab.guacamole.loci.partitioning.UniformPartitioner
import org.hammerlab.guacamole.pileup.{ Pileup, PileupElement }
import org.hammerlab.guacamole.readsets.PartitionedReads
import org.hammerlab.guacamole.readsets.rdd.PartitionedRegionsUtil
import org.hammerlab.guacamole.reference.{ MapBackedReferenceSequence, ReferenceUtil }
import org.hammerlab.guacamole.util.GuacFunSuite

class PileupFlatMapUtilsSuite
  extends GuacFunSuite
    with ReadsRDDUtil
    with PartitionedRegionsUtil
    with LociSetUtil
    with ReferenceUtil {

  register(
    // In some test cases below, we serialize Pileups, which requires PileupElements and MapBackedReferenceSequences.
    classOf[Pileup],
    classOf[Array[Pileup]],

    classOf[PileupElement],
    classOf[Array[PileupElement]],

    // Closed over by PileupElement
    classOf[MapBackedReferenceSequence],
    "org.apache.spark.broadcast.TorrentBroadcast",
    classOf[BroadcastBlockId],
    classOf[Map[_, _]],

    // "test pileup flatmap multiple rdds; skip empty pileups" collects an RDD of Arrays
    classOf[Array[PerSample[_]]],

    // A few cases collect an RDD[Locus]
    classOf[Array[Locus]]
  )

  lazy val reference =
    makeReference(
      ("chr1",  0, "ATCGATCGA" + "N"*90),

      ("chr1", 99, "ACGTACGTACGT" + "N"*500),
      ("chr2", 10, "N"*10)
    )

  def dummyReadsRDD =
    makeReadsRDD(
      ("TCGATCGA", "8M", 1),
      ("TCGATCGA", "8M", 1),
      ("TCGATCGA", "8M", 1)
    )

  def dummyPartitionedReads(lociStr: String, numPartitions: Int): PartitionedReads =
    dummyPartitionedReads(lociStr, Some(numPartitions))

  def dummyPartitionedReads(lociStr: String, numPartitionsOpt: Option[Int] = None): PartitionedReads = {
    val readsRDD = dummyReadsRDD
    partitionReads(
      readsRDD,
      UniformPartitioner(
        numPartitionsOpt
          .getOrElse(
            readsRDD.getNumPartitions
          )
      )
      .partition(lociStr)
    )
  }

  test("test pileup flatmap parallelism 0; create pileups") {

    val partitionedReads = dummyPartitionedReads("chr1:1-9")

    val pileups =
      pileupFlatMapOneSample(
        partitionedReads,
        sampleName = "sampleName",
        skipEmpty = false,
        pileup ⇒ Iterator(pileup),
        reference = reference
      ).collect()

    pileups.length should be(8)
    val firstPileup = pileups.head
    firstPileup.locus should === (1)
    firstPileup.referenceBase should === (T)

    firstPileup.elements.forall(_.readPosition == 0) should be(true)
    firstPileup.elements.forall(_.isMatch) should be(true)

    pileups.forall(_.elements.head.isMatch) should be(true)
  }

  test("test pileup flatmap parallelism 5; create pileups") {

    val partitionedReads = dummyPartitionedReads("chr1:1-9", 5)

    val pileups =
      pileupFlatMapOneSample(
        partitionedReads,
        sampleName = "sampleName",
        skipEmpty = false,
        pileup ⇒ Iterator(pileup),
        reference = reference
      ).collect()

    val firstPileup = pileups.head
    firstPileup.locus should === (1)
    firstPileup.referenceBase should === (T)

    pileups.forall(_.elements.head.isMatch) should be(true)
  }

  test("test pileup flatmap parallelism 5; skip empty pileups") {

    val partitionedReads = dummyPartitionedReads("chr0:5-10,chr1:0-100,chr2:0-1000,chr2:5000-6000", 5)

    val loci =
      pileupFlatMapOneSample(
        partitionedReads,
        sampleName = "sampleName",
        skipEmpty = true,
        pileup ⇒ Iterator(pileup.locus),
        reference = reference
      )
      .collect

    loci should === (Array(1, 2, 3, 4, 5, 6, 7, 8))
  }

  test("test pileup flatmap two rdds; skip empty pileups") {
    val reads1 =
      makeReadsRDD(
        sampleId = 0,
        ("TCGATCGA", "8M", 1),
        ("TCGATCGA", "8M", 1),
        ("TCGATCGA", "8M", 1),
        ("GGGGGGGG", "8M", 100),
        ("GGGGGGGG", "8M", 100),
        ("GGGGGGGG", "8M", 100)
      )

    val reads2 =
      makeReadsRDD(
        sampleId = 1,
        ("AAAAAAAA", "8M", 1),
        ("CCCCCCCC", "8M", 1),
        ("TTTTTTTT", "8M", 1),
        ("XXX", "3M", 99)
      )

    val partitionedReads =
      partitionReads(
        reads1 ++ reads2,
        UniformPartitioner(1).partition("chr0:0-1000,chr1:1-500,chr2:10-20")
      )

    val loci =
      pileupFlatMapTwoSamples(
        partitionedReads,
        sample1Name = "sample1",
        sample2Name = "sample2",
        skipEmpty = true,
        (pileup1, _) ⇒ Iterator(pileup1.locus),
        reference = reference
      )
      .collect

    loci should === (Array(1, 2, 3, 4, 5, 6, 7, 8, 99, 100, 101, 102, 103, 104, 105, 106, 107))
  }

  test("test pileup flatmap multiple rdds; skip empty pileups") {
    val reads1 =
      makeReadsRDD(
        sampleId = 0,
        ("TCGATCGA", "8M", 1),
        ("TCGATCGA", "8M", 1),
        ("TCGATCGA", "8M", 1),
        ("GGGGGGGG", "8M", 100),
        ("GGGGGGGG", "8M", 100),
        ("GGGGGGGG", "8M", 100)
      )

    val reads2 =
      makeReadsRDD(
        sampleId = 1,
        ("AAAAAAAA", "8M", 1),
        ("CCCCCCCC", "8M", 1),
        ("TTTTTTTT", "8M", 1),
        ("XYX", "3M", 99)
      )

    val reads3 =
      makeReadsRDD(
        sampleId = 2,
        ("AAGGCCTT", "8M", 1),
        ("GGAATTCC", "8M", 1),
        ("GGGGGGGG", "8M", 1),
        ("XZX", "3M", 99)
      )

    val loci = "chr1:1-500,chr2:10-20"

    val reads = reads1 ++ reads2 ++ reads3

    val resultPlain =
      pileupFlatMapMultipleSamples[PerSample[Iterable[Bases]]](
        sampleNames = Vector("a", "b", "c"),
        partitionReads(reads, UniformPartitioner(1).partition(loci)),
        skipEmpty = true,
        pileupsToElementStrings,
        reference = reference
      )
      .collect
      .map(_.map(_.toSeq).toSeq)

    val resultParallelized =
      pileupFlatMapMultipleSamples[PerSample[Iterable[Bases]]](
        sampleNames = Vector("a", "b", "c"),
        partitionReads(reads, UniformPartitioner(800).partition(loci)),
        skipEmpty = true,
        pileupsToElementStrings,
        reference = reference
      )
      .collect
      .map(_.map(_.toSeq).toSeq)

    val resultWithEmpty =
      pileupFlatMapMultipleSamples[PerSample[Iterable[Bases]]](
        sampleNames = Vector("a", "b", "c"),
        partitionReads(reads, UniformPartitioner(5).partition(loci)),
        skipEmpty = false,
        pileupsToElementStrings,
        reference = reference
      )
      .collect
      .map(_.map(_.toSeq).toSeq)

    resultPlain should === (resultParallelized)

    // TODO(ryan): move to BasesUtil
    implicit def seqSeqBases(s: Seq[Seq[Base]]): Seq[Seq[Bases]] = s.map(_.map(Bases(_)))

    resultWithEmpty(0) should === (resultPlain(0))
    resultWithEmpty(1) should === (resultPlain(1))
    resultWithEmpty(2) should === (resultPlain(2))
    resultWithEmpty(3) should === (resultPlain(3))
    resultWithEmpty(35) should === (Seq(Seq[Bases](), Seq[Bases](), Seq[Bases]()))

    resultPlain(0) should === (Seq(Seq(T, T, T), Seq(A, C, T), Seq(A, G, G)))
    resultPlain(1) should === (Seq(Seq(C, C, C), Seq(A, C, T), Seq(A, G, G)))
    resultPlain(2) should === (Seq(Seq(G, G, G), Seq(A, C, T), Seq(G, A, G)))
    resultPlain(3) should === (Seq(Seq(A, A, A), Seq(A, C, T), Seq(G, A, G)))

    resultPlain(8) should === (Seq(Seq(), Seq(N), Seq(N)))
    resultPlain(9) should === (Seq(Seq(G, G, G), Seq(N), Seq(N)))
    resultPlain(10) should === (Seq(Seq(G, G, G), Seq(N), Seq(N)))
  }

  test("test pileup flatmap parallelism 5; create pileup elements") {

    val partitionedReads = dummyPartitionedReads("chr1:1-9", 5)

    val pileups =
      pileupFlatMapOneSample[PileupElement](
        partitionedReads,
        sampleName = "sampleName",
        skipEmpty = false,
        _.elements.toIterator,
        reference = makeReference("chr1", 1, "TCGATCGA")
      ).collect()

    pileups.length should be(24)
    pileups.forall(_.isMatch) should be(true)
  }

  test("test two-rdd pileup flatmap; create pileup elements") {
    val reads1 =
      makeReadsRDD(
        sampleId = 0,
        ("TCGATCGA", "8M", 1),
        ("TCGATCGA", "8M", 1),
        ("TCGATCGA", "8M", 1),
        ("GGGGGGGG", "8M", 100),
        ("GGGGGGGG", "8M", 100),
        ("GGGGGGGG", "8M", 100)
      )

    val reads2 =
      makeReadsRDD(
        sampleId = 1,
        ("TCGATCGA", "8M", 1),
        ("TCGATCGA", "8M", 1),
        ("TCGATCGA", "8M", 1),
        ("AGG", "3M", 99)
      )

    val partitionedReads =
      partitionReads(
        reads1 ++ reads2,
        UniformPartitioner(1000).partition("chr1:1-500")
      )

    val elements =
      pileupFlatMapTwoSamples[PileupElement](
        partitionedReads,
        sample1Name = "sample1",
        sample2Name = "sample2",
        skipEmpty = false,
        (pileup1, pileup2) ⇒ (pileup1.elements ++ pileup2.elements).toIterator,
        reference = makeReference("chr1", 0, "ATCGATCGA" + "N"*90 + "AGGGGGGGGGG" + "N"*500)
      )
      .collect()
      .toVector

    elements.map(_.isMatch) should be(List.fill(elements.length)(true))
    assert(
      Bases(elements.flatMap(_.sequencedBases)) ===
        "TTTTTTCCCCCCGGGGGGAAAAAATTTTTTCCCCCCGGGGGGAAAAAAAGGGGGGGGGGGGGGGGGGGGGGGGGG"
    )
  }

  test("test pileup flatmap parallelism 5; create pileup elements; with indel") {

    val reads =
      makeReadsRDD(
        ("TCGATCGA", "8M", 1),
        ("TCGATCGA", "8M", 1),
        ("TCGACCCTCGA", "4M3I4M", 1)
      )

    val pileups =
      pileupFlatMapOneSample[PileupElement](
        partitionReads(
          reads,
          UniformPartitioner(5).partition("chr1:1-12")
        ),
        sampleName = "sampleName",
        skipEmpty = false,
        _.elements.toIterator,
        reference = makeReference("chr1", 1, "ATCGATCGATC")
      ).collect()

    pileups.length should be(24)
    val insertionPileups = pileups.filter(_.isInsertion)
    insertionPileups.length should be(1)
  }
}

private object Util {
  /**
   * This helper function is in its own object here to avoid serializing `PileupFlatMapUtilsSuite`, which is not
   * serializable due the `AssertionsHelper` nested-class in [[org.scalatest.Assertions]].
   */
  def pileupsToElementStrings(pileups: PerSample[Pileup]): Iterator[PerSample[Iterable[Bases]]] =
    Iterator(pileups.map(_.elements.map(_.sequencedBases)))
}
