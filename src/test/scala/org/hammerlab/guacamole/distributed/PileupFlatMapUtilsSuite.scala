/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hammerlab.guacamole.distributed

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.storage.BroadcastBlockId
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.pileup.{Pileup, PileupElement}
import org.hammerlab.guacamole.reference.ReferenceBroadcast.MapBackedReferenceSequence
import org.hammerlab.guacamole.util.{AssertBases, Bases, GuacFunSuite, KryoTestRegistrar, TestUtil}

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
    kryo.register(classOf[Array[Seq[_]]])
  }
}

class PileupFlatMapUtilsSuite extends GuacFunSuite {

  override def registrar: String = "org.hammerlab.guacamole.distributed.PileupFlatMapUtilsSuiteRegistrar"

  test("test pileup flatmap parallelism 0; create pileups") {

    val reads = sc.parallelize(Seq(
      TestUtil.makeRead("TCGATCGA", "8M", 1),
      TestUtil.makeRead("TCGATCGA", "8M", 1),
      TestUtil.makeRead("TCGATCGA", "8M", 1)))

    val pileups =
      PileupFlatMapUtils.pileupFlatMap[Pileup](
        reads,
        LociPartitionUtils.partitionLociUniformly(reads.partitions.length, LociSet("chr1:1-9")),
        skipEmpty = false,
        pileup => Iterator(pileup),
        reference = TestUtil.makeReference(sc, Seq(("chr1", 0, "ATCGATCGA")))
      ).collect()

    pileups.length should be(8)
    val firstPileup = pileups.head
    firstPileup.locus should be(1L)
    firstPileup.referenceBase should be(Bases.T)

    firstPileup.elements.forall(_.readPosition == 0L) should be(true)
    firstPileup.elements.forall(_.isMatch) should be(true)

    pileups.forall(_.head.isMatch) should be(true)

  }

  test("test pileup flatmap parallelism 5; create pileups") {

    val reads = sc.parallelize(Seq(
      TestUtil.makeRead("TCGATCGA", "8M", 1),
      TestUtil.makeRead("TCGATCGA", "8M", 1),
      TestUtil.makeRead("TCGATCGA", "8M", 1)))

    val pileups =
      PileupFlatMapUtils.pileupFlatMap[Pileup](
        reads,
        LociPartitionUtils.partitionLociUniformly(5, LociSet("chr1:1-9")),
        skipEmpty = false,
        pileup => Iterator(pileup),
        reference = TestUtil.makeReference(sc, Seq(("chr1", 0, "ATCGATCGA")))
      ).collect()

    val firstPileup = pileups.head
    firstPileup.locus should be(1L)
    firstPileup.referenceBase should be(Bases.T)

    pileups.forall(_.head.isMatch) should be(true)
  }

  test("test pileup flatmap parallelism 5; skip empty pileups") {
    val reads = sc.parallelize(Seq(
      TestUtil.makeRead("TCGATCGA", "8M", 1),
      TestUtil.makeRead("TCGATCGA", "8M", 1),
      TestUtil.makeRead("TCGATCGA", "8M", 1)))

    val loci =
      PileupFlatMapUtils.pileupFlatMap[Long](
        reads,
        LociPartitionUtils.partitionLociUniformly(5, LociSet("chr0:5-10,chr1:0-100,chr2:0-1000,chr2:5000-6000")),
        skipEmpty = true,
        pileup => Iterator(pileup.locus),
        reference = TestUtil.makeReference(sc, Seq(("chr1", 0, "ATCGATCGA")))
      ).collect
    loci should equal(Array(1, 2, 3, 4, 5, 6, 7, 8))
  }

  test("test pileup flatmap two rdds; skip empty pileups") {
    val reads1 = sc.parallelize(Seq(
      TestUtil.makeRead("TCGATCGA", "8M", 1),
      TestUtil.makeRead("TCGATCGA", "8M", 1),
      TestUtil.makeRead("TCGATCGA", "8M", 1),
      TestUtil.makeRead("GGGGGGGG", "8M", 100),
      TestUtil.makeRead("GGGGGGGG", "8M", 100),
      TestUtil.makeRead("GGGGGGGG", "8M", 100)))

    val reads2 = sc.parallelize(Seq(
      TestUtil.makeRead("AAAAAAAA", "8M", 1),
      TestUtil.makeRead("CCCCCCCC", "8M", 1),
      TestUtil.makeRead("TTTTTTTT", "8M", 1),
      TestUtil.makeRead("XXX", "3M", 99)))

    val loci =
      PileupFlatMapUtils.pileupFlatMapTwoRDDs[Long](
        reads1,
        reads2,
        LociPartitionUtils.partitionLociUniformly(1, LociSet("chr0:0-1000,chr1:1-500,chr2:10-20")),
        skipEmpty = true,
        (pileup1, _) => (Iterator(pileup1.locus)),
        reference = TestUtil.makeReference(sc, Seq(("chr1", 0, "ATCGATCGA")))
      ).collect
    loci should equal(Seq(1, 2, 3, 4, 5, 6, 7, 8, 99, 100, 101, 102, 103, 104, 105, 106, 107))
  }

  test("test pileup flatmap multiple rdds; skip empty pileups") {
    val reads1 = sc.parallelize(Seq(
      TestUtil.makeRead("TCGATCGA", "8M", 1),
      TestUtil.makeRead("TCGATCGA", "8M", 1),
      TestUtil.makeRead("TCGATCGA", "8M", 1),
      TestUtil.makeRead("GGGGGGGG", "8M", 100),
      TestUtil.makeRead("GGGGGGGG", "8M", 100),
      TestUtil.makeRead("GGGGGGGG", "8M", 100)))

    val reads2 = sc.parallelize(Seq(
      TestUtil.makeRead("AAAAAAAA", "8M", 1),
      TestUtil.makeRead("CCCCCCCC", "8M", 1),
      TestUtil.makeRead("TTTTTTTT", "8M", 1),
      TestUtil.makeRead("XYX", "3M", 99)))

    val reads3 = sc.parallelize(Seq(
      TestUtil.makeRead("AAGGCCTT", "8M", 1),
      TestUtil.makeRead("GGAATTCC", "8M", 1),
      TestUtil.makeRead("GGGGGGGG", "8M", 1),
      TestUtil.makeRead("XZX", "3M", 99)))

    val resultPlain =
      PileupFlatMapUtils.pileupFlatMapMultipleRDDs[Seq[Seq[String]]](
        Vector(reads1, reads2, reads3),
        LociPartitionUtils.partitionLociUniformly(1, LociSet("chr1:1-500,chr2:10-20")),
        skipEmpty = true,
        pileups => Iterator(pileups.map(_.elements.map(p => Bases.basesToString(p.sequencedBases)))),
        reference = TestUtil.makeReference(sc, Seq(("chr1", 0, "ATCGATCGA")))
      ).collect.map(_.toList)

    val resultParallelized = PileupFlatMapUtils.pileupFlatMapMultipleRDDs[Seq[Seq[String]]](
      Vector(reads1, reads2, reads3),
      LociPartitionUtils.partitionLociUniformly(800, LociSet("chr0:0-100,chr1:1-500,chr2:10-20")),
      skipEmpty = true,
      pileups => Iterator(pileups.map(_.elements.map(p => Bases.basesToString(p.sequencedBases)))),
      reference = TestUtil.makeReference(sc, Seq(("chr1", 0, "ATCGATCGA")))
    ).collect.map(_.toList)

    val resultWithEmpty =
      PileupFlatMapUtils.pileupFlatMapMultipleRDDs[Seq[Seq[String]]](
        Vector(reads1, reads2, reads3),
        LociPartitionUtils.partitionLociUniformly(5, LociSet("chr1:1-500,chr2:10-20")),
        skipEmpty = false,
        pileups => Iterator(pileups.map(_.elements.map(p => Bases.basesToString(p.sequencedBases)))),
        reference = TestUtil.makeReference(sc, Seq(("chr1", 0, "ATCGATCGA"), ("chr2", 0, "")))
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

    val reads = sc.parallelize(Seq(
      TestUtil.makeRead("TCGATCGA", "8M", 1),
      TestUtil.makeRead("TCGATCGA", "8M", 1),
      TestUtil.makeRead("TCGATCGA", "8M", 1)))

    val pileups =
      PileupFlatMapUtils.pileupFlatMap[PileupElement](
        reads,
        LociPartitionUtils.partitionLociUniformly(5, LociSet("chr1:1-9")),
        skipEmpty = false,
        _.elements.toIterator,
        reference = TestUtil.makeReference(sc, Seq(("chr1", 1, "TCGATCGA")))
      ).collect()

    pileups.length should be(24)
    pileups.forall(_.isMatch) should be(true)
  }

  test("test two-rdd pileup flatmap; create pileup elements") {
    val reads1 = sc.parallelize(Seq(
      TestUtil.makeRead("TCGATCGA", "8M", 1),
      TestUtil.makeRead("TCGATCGA", "8M", 1),
      TestUtil.makeRead("TCGATCGA", "8M", 1),
      TestUtil.makeRead("GGGGGGGG", "8M", 100),
      TestUtil.makeRead("GGGGGGGG", "8M", 100),
      TestUtil.makeRead("GGGGGGGG", "8M", 100)))

    val reads2 = sc.parallelize(Seq(
      TestUtil.makeRead("TCGATCGA", "8M", 1),
      TestUtil.makeRead("TCGATCGA", "8M", 1),
      TestUtil.makeRead("TCGATCGA", "8M", 1),
      TestUtil.makeRead("AGG", "3M", 99)))

    val elements =
      PileupFlatMapUtils.pileupFlatMapTwoRDDs[PileupElement](
        reads1,
        reads2,
        LociPartitionUtils.partitionLociUniformly(1000, LociSet("chr1:1-500")),
        skipEmpty = false,
        (pileup1, pileup2) => (pileup1.elements ++ pileup2.elements).toIterator,
        reference = TestUtil.makeReference(sc, Seq(("chr1", 0, "ATCGATCGA" + "N" * 90 + "AGGGGGGGGGG")))
      ).collect()

    elements.map(_.isMatch) should equal(List.fill(elements.length)(true))
    AssertBases(
      elements.flatMap(_.sequencedBases).toSeq,
      "TTTTTTCCCCCCGGGGGGAAAAAATTTTTTCCCCCCGGGGGGAAAAAAAGGGGGGGGGGGGGGGGGGGGGGGGGG"
    )
  }

  test("test pileup flatmap parallelism 5; create pileup elements; with indel") {

    val reads = sc.parallelize(Seq(
      TestUtil.makeRead("TCGATCGA", "8M", 1),
      TestUtil.makeRead("TCGATCGA", "8M", 1),
      TestUtil.makeRead("TCGACCCTCGA", "4M3I4M", 1)))

    val pileups =
      PileupFlatMapUtils.pileupFlatMap[PileupElement](
        reads,
        LociPartitionUtils.partitionLociUniformly(5, LociSet("chr1:1-12")),
        skipEmpty = false,
        _.elements.toIterator,
        reference = TestUtil.makeReference(sc, Seq(("chr1", 0, "ATCGATCGA ")))
      ).collect()

    pileups.length should be(24)
    val insertionPileups = pileups.filter(_.isInsertion)
    insertionPileups.length should be(1)
  }
}
