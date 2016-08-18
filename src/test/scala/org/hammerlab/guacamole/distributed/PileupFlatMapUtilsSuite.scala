package org.hammerlab.guacamole.distributed

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.storage.BroadcastBlockId
import org.hammerlab.guacamole.distributed.PileupFlatMapUtils.{pileupFlatMapMultipleSamples, pileupFlatMapOneSample, pileupFlatMapTwoSamples}
import org.hammerlab.guacamole.distributed.Util.pileupsToElementStrings
import org.hammerlab.guacamole.loci.partitioning.UniformPartitioner
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.pileup.{Pileup, PileupElement}
import org.hammerlab.guacamole.readsets.rdd.{PartitionedRegionsUtil, ReadsRDDUtil}
import org.hammerlab.guacamole.readsets.{PartitionedReads, PerSample}
import org.hammerlab.guacamole.reference.ReferenceBroadcast.MapBackedReferenceSequence
import org.hammerlab.guacamole.reference.ReferenceUtil
import org.hammerlab.guacamole.util.{AssertBases, Bases, GuacFunSuite, KryoTestRegistrar}

class PileupFlatMapUtilsSuiteRegistrar extends KryoTestRegistrar {
  override def registerTestClasses(kryo: Kryo): Unit = {
    // In some test cases below, we serialize Pileups, which requires PileupElements and MapBackedReferenceSequences.
    kryo.register(classOf[Pileup])
    kryo.register(classOf[Array[Pileup]])

    kryo.register(classOf[PileupElement])
    kryo.register(classOf[Array[PileupElement]])

    // Closed over by PileupElement
    kryo.register(classOf[MapBackedReferenceSequence])
    kryo.register(Class.forName("org.apache.spark.broadcast.TorrentBroadcast"))
    kryo.register(classOf[BroadcastBlockId])
    kryo.register(classOf[Map[_, _]])

    // "test pileup flatmap multiple rdds; skip empty pileups" collects an RDD of Arrays
    kryo.register(classOf[Array[PerSample[_]]])
  }
}

private object Util {
  // This helper function is in its own object here to avoid serializing `PileupFlatMapUtilsSuite`, which is not
  // serializable due to mixing in `Matchers`.
  def pileupsToElementStrings(pileups: PerSample[Pileup]): Iterator[PerSample[Iterable[String]]] =
    Iterator(pileups.map(_.elements.map(p => Bases.basesToString(p.sequencedBases))))
}

class PileupFlatMapUtilsSuite
  extends GuacFunSuite
    with ReadsRDDUtil
    with PartitionedRegionsUtil
    with ReferenceUtil {

  override def registrar: String = "org.hammerlab.guacamole.distributed.PileupFlatMapUtilsSuiteRegistrar"

  lazy val reference = makeReference(sc, Seq(("chr1", 0, "ATCGATCGA")))

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
      Vector(readsRDD),
      UniformPartitioner(
        numPartitionsOpt
          .getOrElse(
            readsRDD.getNumPartitions
          )
      ).partition(
        LociSet(lociStr)
      )
    )
  }

  test("test pileup flatmap parallelism 0; create pileups") {

    val partitionedReads = dummyPartitionedReads("chr1:1-9")

    val pileups =
      pileupFlatMapOneSample(
        partitionedReads,
        skipEmpty = false,
        pileup => Iterator(pileup),
        reference = reference
      ).collect()

    pileups.length should be(8)
    val firstPileup = pileups.head
    firstPileup.locus should be(1L)
    firstPileup.referenceBase should be(Bases.T)

    firstPileup.elements.forall(_.readPosition == 0L) should be(true)
    firstPileup.elements.forall(_.isMatch) should be(true)

    pileups.forall(_.elements.head.isMatch) should be(true)
  }

  test("test pileup flatmap parallelism 5; create pileups") {

    val partitionedReads = dummyPartitionedReads("chr1:1-9", 5)

    val pileups =
      pileupFlatMapOneSample(
        partitionedReads,
        skipEmpty = false,
        pileup => Iterator(pileup),
        reference = reference
      ).collect()

    val firstPileup = pileups.head
    firstPileup.locus should be(1L)
    firstPileup.referenceBase should be(Bases.T)

    pileups.forall(_.elements.head.isMatch) should be(true)
  }

  test("test pileup flatmap parallelism 5; skip empty pileups") {

    val partitionedReads = dummyPartitionedReads("chr0:5-10,chr1:0-100,chr2:0-1000,chr2:5000-6000", 5)

    val loci =
      pileupFlatMapOneSample(
        partitionedReads,
        skipEmpty = true,
        pileup => Iterator(pileup.locus),
        reference = reference
      ).collect

    loci should equal(Array(1, 2, 3, 4, 5, 6, 7, 8))
  }

  test("test pileup flatmap two rdds; skip empty pileups") {
    val reads1 =
      makeReadsRDD(
        ("TCGATCGA", "8M", 1),
        ("TCGATCGA", "8M", 1),
        ("TCGATCGA", "8M", 1),
        ("GGGGGGGG", "8M", 100),
        ("GGGGGGGG", "8M", 100),
        ("GGGGGGGG", "8M", 100)
      )

    val reads2 =
      makeReadsRDD(
        ("AAAAAAAA", "8M", 1),
        ("CCCCCCCC", "8M", 1),
        ("TTTTTTTT", "8M", 1),
        ("XXX", "3M", 99)
      )

    val partitionedReads =
      partitionReads(
        Vector(reads1, reads2),
        UniformPartitioner(1).partition(LociSet("chr0:0-1000,chr1:1-500,chr2:10-20"))
      )

    val loci =
      pileupFlatMapTwoSamples(
        partitionedReads,
        skipEmpty = true,
        (pileup1, _) => Iterator(pileup1.locus),
        reference = reference
      ).collect

    loci should equal(Seq(1, 2, 3, 4, 5, 6, 7, 8, 99, 100, 101, 102, 103, 104, 105, 106, 107))
  }

  test("test pileup flatmap multiple rdds; skip empty pileups") {
    val reads1 =
      makeReadsRDD(
        ("TCGATCGA", "8M", 1),
        ("TCGATCGA", "8M", 1),
        ("TCGATCGA", "8M", 1),
        ("GGGGGGGG", "8M", 100),
        ("GGGGGGGG", "8M", 100),
        ("GGGGGGGG", "8M", 100)
      )

    val reads2 =
      makeReadsRDD(
        ("AAAAAAAA", "8M", 1),
        ("CCCCCCCC", "8M", 1),
        ("TTTTTTTT", "8M", 1),
        ("XYX", "3M", 99)
      )

    val reads3 =
      makeReadsRDD(
        ("AAGGCCTT", "8M", 1),
        ("GGAATTCC", "8M", 1),
        ("GGGGGGGG", "8M", 1),
        ("XZX", "3M", 99)
      )

    val loci = LociSet("chr1:1-500,chr2:10-20")

    val readsRDDs = Vector(reads1, reads2, reads3)

    val resultPlain =
      pileupFlatMapMultipleSamples[PerSample[Iterable[String]]](
        partitionReads(readsRDDs, UniformPartitioner(1).partition(loci)),
        skipEmpty = true,
        pileupsToElementStrings,
        reference = reference
      ).collect.map(_.toList)

    val resultParallelized =
      pileupFlatMapMultipleSamples[PerSample[Iterable[String]]](
        partitionReads(readsRDDs, UniformPartitioner(800).partition(loci)),
        skipEmpty = true,
        pileupsToElementStrings,
        reference = reference
      ).collect.map(_.toList)

    val resultWithEmpty =
      pileupFlatMapMultipleSamples[PerSample[Iterable[String]]](
        partitionReads(readsRDDs, UniformPartitioner(5).partition(loci)),
        skipEmpty = false,
        pileupsToElementStrings,
        reference = makeReference(sc, Seq(("chr1", 0, "ATCGATCGA"), ("chr2", 0, "")))
      ).collect.map(_.toList)

    resultPlain should equal(resultParallelized)

    resultWithEmpty(0) should equal(resultPlain(0))
    resultWithEmpty(1) should equal(resultPlain(1))
    resultWithEmpty(2) should equal(resultPlain(2))
    resultWithEmpty(3) should equal(resultPlain(3))
    resultWithEmpty(35) should equal(Seq(Seq(), Seq(), Seq()))

    resultPlain(0) should equal(Seq(Seq("T", "T", "T"), Seq("A", "C", "T"), Seq("A", "G", "G")))
    resultPlain(1) should equal(Seq(Seq("C", "C", "C"), Seq("A", "C", "T"), Seq("A", "G", "G")))
    resultPlain(2) should equal(Seq(Seq("G", "G", "G"), Seq("A", "C", "T"), Seq("G", "A", "G")))
    resultPlain(3) should equal(Seq(Seq("A", "A", "A"), Seq("A", "C", "T"), Seq("G", "A", "G")))

    resultPlain(8) should equal(Seq(Seq(), Seq("X"), Seq("X")))
    resultPlain(9) should equal(Seq(Seq("G", "G", "G"), Seq("Y"), Seq("Z")))
    resultPlain(10) should equal(Seq(Seq("G", "G", "G"), Seq("X"), Seq("X")))
  }

  test("test pileup flatmap parallelism 5; create pileup elements") {

    val partitionedReads = dummyPartitionedReads("chr1:1-9", 5)

    val pileups =
      pileupFlatMapOneSample[PileupElement](
        partitionedReads,
        skipEmpty = false,
        _.elements.toIterator,
        reference = makeReference(sc, Seq(("chr1", 1, "TCGATCGA")))
      ).collect()

    pileups.length should be(24)
    pileups.forall(_.isMatch) should be(true)
  }

  test("test two-rdd pileup flatmap; create pileup elements") {
    val reads1 =
      makeReadsRDD(
        ("TCGATCGA", "8M", 1),
        ("TCGATCGA", "8M", 1),
        ("TCGATCGA", "8M", 1),
        ("GGGGGGGG", "8M", 100),
        ("GGGGGGGG", "8M", 100),
        ("GGGGGGGG", "8M", 100)
      )

    val reads2 =
      makeReadsRDD(
        ("TCGATCGA", "8M", 1),
        ("TCGATCGA", "8M", 1),
        ("TCGATCGA", "8M", 1),
        ("AGG", "3M", 99)
      )

    val partitionedReads =
      partitionReads(
        Vector(reads1, reads2),
        UniformPartitioner(1000).partition(LociSet("chr1:1-500"))
      )

    val elements =
      pileupFlatMapTwoSamples[PileupElement](
        partitionedReads,
        skipEmpty = false,
        (pileup1, pileup2) => (pileup1.elements ++ pileup2.elements).toIterator,
        reference = makeReference(sc, Seq(("chr1", 0, "ATCGATCGA" + "N" * 90 + "AGGGGGGGGGG")))
      ).collect()

    elements.map(_.isMatch) should equal(List.fill(elements.length)(true))
    AssertBases(
      elements.flatMap(_.sequencedBases).toSeq,
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
          Vector(reads),
          UniformPartitioner(5).partition(LociSet("chr1:1-12"))
        ),
        skipEmpty = false,
        _.elements.toIterator,
        reference = makeReference(sc, Seq(("chr1", 0, "ATCGATCGA")))
      ).collect()

    pileups.length should be(24)
    val insertionPileups = pileups.filter(_.isInsertion)
    insertionPileups.length should be(1)
  }
}
