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

package org.hammerlab.guacamole

import org.apache.spark.rdd.RDD
import org.bdgenomics.formats.avro.{ Genotype, GenotypeAllele }
import org.hammerlab.guacamole.util.{ TestUtil, GuacFunSuite }
import TestUtil.assertBases
import org.hammerlab.guacamole.commands.GermlineThreshold
import org.hammerlab.guacamole.pileup.{ Pileup, PileupElement }
import org.hammerlab.guacamole.reads.MappedRead
import org.hammerlab.guacamole.windowing.SlidingWindow
import org.scalatest.Matchers

import scala.collection.JavaConversions._

class DistributedUtilSuite extends GuacFunSuite with Matchers {

  test("partitionLociUniformly") {
    val set = LociSet.parse("chr21:100-200,chr20:0-10,chr20:8-15,chr20:100-121,empty:10-10").result
    val result1 = DistributedUtil.partitionLociUniformly(1, set).asInverseMap
    result1(0) should equal(set)

    val result2 = DistributedUtil.partitionLociUniformly(2, set).asInverseMap
    result2(0).count should equal(set.count / 2)
    result2(1).count should equal(set.count / 2)
    result2(0) should not equal (result2(1))
    result2(0).union(result2(1)) should equal(set)

    val result3 = DistributedUtil.partitionLociUniformly(4, LociSet.parse("chrM:0-16571").result())
    result3.toString should equal("chrM:0-4143=0,chrM:4143-8286=1,chrM:8286-12428=2,chrM:12428-16571=3")

    val result4 = DistributedUtil.partitionLociUniformly(100, LociSet.parse("chrM:1000-1100").result)
    val expectedBuilder4 = LociMap.newBuilder[Long]
    for (i <- 0 until 100) {
      expectedBuilder4.put("chrM", i + 1000, i + 1001, i)
    }
    result4 should equal(expectedBuilder4.result)

    val result5 = DistributedUtil.partitionLociUniformly(3, LociSet.parse("chrM:0-10").result)
    result5.toString should equal("chrM:0-3=0,chrM:3-7=1,chrM:7-10=2")

    val result6 = DistributedUtil.partitionLociUniformly(4, LociSet.parse("chrM:0-3").result)
    result6.toString should equal("chrM:0-1=0,chrM:1-2=1,chrM:2-3=2")

    val result7 = DistributedUtil.partitionLociUniformly(4, LociSet.parse("empty:10-10").result)
    result7.toString should equal("")
  }

  test("partitionLociUniformly performance") {
    // These two calls should not take a noticeable amount of time.
    val bigSet = LociSet.parse("chr21:0-3000000000").result
    DistributedUtil.partitionLociUniformly(2000, bigSet).asInverseMap

    val giantSet = LociSet.parse(
      "1:0-249250621,10:0-135534747,11:0-135006516,12:0-133851895,13:0-115169878,14:0-107349540,15:0-102531392,16:0-90354753,17:0-81195210,18:0-78077248,19:0-59128983,2:0-243199373,20:0-63025520,21:0-48129895,22:0-51304566,3:0-198022430,4:0-191154276,5:0-180915260,6:0-171115067,7:0-159138663,8:0-146364022,9:0-141213431,GL000191.1:0-106433,GL000192.1:0-547496,GL000193.1:0-189789,GL000194.1:0-191469,GL000195.1:0-182896,GL000196.1:0-38914,GL000197.1:0-37175,GL000198.1:0-90085,GL000199.1:0-169874,GL000200.1:0-187035,GL000201.1:0-36148,GL000202.1:0-40103,GL000203.1:0-37498,GL000204.1:0-81310,GL000205.1:0-174588,GL000206.1:0-41001,GL000207.1:0-4262,GL000208.1:0-92689,GL000209.1:0-159169,GL000210.1:0-27682,GL000211.1:0-166566,GL000212.1:0-186858,GL000213.1:0-164239,GL000214.1:0-137718,GL000215.1:0-172545,GL000216.1:0-172294,GL000217.1:0-172149,GL000218.1:0-161147,GL000219.1:0-179198,GL000220.1:0-161802,GL000221.1:0-155397,GL000222.1:0-186861,GL000223.1:0-180455,GL000224.1:0-179693,GL000225.1:0-211173,GL000226.1:0-15008,GL000227.1:0-128374,GL000228.1:0-129120,GL000229.1:0-19913,GL000230.1:0-43691,GL000231.1:0-27386,GL000232.1:0-40652,GL000233.1:0-45941,GL000234.1:0-40531,GL000235.1:0-34474,GL000236.1:0-41934,GL000237.1:0-45867,GL000238.1:0-39939,GL000239.1:0-33824,GL000240.1:0-41933,GL000241.1:0-42152,GL000242.1:0-43523,GL000243.1:0-43341,GL000244.1:0-39929,GL000245.1:0-36651,GL000246.1:0-38154,GL000247.1:0-36422,GL000248.1:0-39786,GL000249.1:0-38502,MT:0-16569,NC_007605:0-171823,X:0-155270560,Y:0-59373566,hs37d5:0-35477943")
      .result
    DistributedUtil.partitionLociUniformly(2000, giantSet).asInverseMap
  }

  sparkTest("partitionLociByApproximateReadDepth") {
    def makeRead(start: Long, length: Long) = {
      TestUtil.makeRead("A" * length.toInt, "%sM".format(length), start, "chr1")
    }
    def pairsToReads(pairs: Seq[(Long, Long)]): RDD[MappedRead] = {
      sc.parallelize(pairs.map(pair => makeRead(pair._1, pair._2)))
    }
    {
      val reads = pairsToReads(Seq(
        (5L, 1L),
        (6L, 1L),
        (7L, 1L),
        (8L, 1L)))
      val loci = LociSet.parse("chr1:0-100").result
      val result = DistributedUtil.partitionLociByApproximateDepth(2, loci, 100, reads)
      result.toString should equal("chr1:0-7=0,chr1:7-100=1")
    }
  }

  sparkTest("test pileup flatmap parallelism 0; create pileups") {

    val reads = sc.parallelize(Seq(
      TestUtil.makeRead("TCGATCGA", "8M", 1),
      TestUtil.makeRead("TCGATCGA", "8M", 1),
      TestUtil.makeRead("TCGATCGA", "8M", 1)))

    val pileups = DistributedUtil.pileupFlatMap[Pileup](
      reads,
      DistributedUtil.partitionLociUniformly(reads.partitions.length, LociSet.parse("chr1:1-9").result),
      false, // don't skip empty pileups
      pileup => Seq(pileup).iterator,
      reference = TestUtil.makeReference(sc, Seq(("chr1", 0, "ATCGATCGA")))).collect()

    pileups.length should be(8)
    val firstPileup = pileups.head
    firstPileup.locus should be(1L)
    firstPileup.referenceBase should be(Bases.T)

    firstPileup.elements.forall(_.readPosition == 0L) should be(true)
    firstPileup.elements.forall(_.isMatch) should be(true)

    pileups.forall(p => p.head.isMatch) should be(true)

  }

  sparkTest("test pileup flatmap parallelism 5; create pileups") {

    val reads = sc.parallelize(Seq(
      TestUtil.makeRead("TCGATCGA", "8M", 1),
      TestUtil.makeRead("TCGATCGA", "8M", 1),
      TestUtil.makeRead("TCGATCGA", "8M", 1)))

    val pileups = DistributedUtil.pileupFlatMap[Pileup](
      reads,
      DistributedUtil.partitionLociUniformly(5, LociSet.parse("chr1:1-9").result),
      false, // don't skip empty pileups
      pileup => Seq(pileup).iterator,
      reference = TestUtil.makeReference(sc, Seq(("chr1", 0, "ATCGATCGA")))).collect()

    val firstPileup = pileups.head
    firstPileup.locus should be(1L)
    firstPileup.referenceBase should be(Bases.T)

    pileups.forall(p => p.head.isMatch) should be(true)
  }

  sparkTest("test pileup flatmap parallelism 5; skip empty pileups") {
    val reads = sc.parallelize(Seq(
      TestUtil.makeRead("TCGATCGA", "8M", 1),
      TestUtil.makeRead("TCGATCGA", "8M", 1),
      TestUtil.makeRead("TCGATCGA", "8M", 1)))

    val pileups = DistributedUtil.pileupFlatMap[Long](
      reads,
      DistributedUtil.partitionLociUniformly(5, LociSet.parse("chr0:5-10,chr1:0-100,chr2:0-1000,chr2:5000-6000").result),
      true, // skip empty pileups
      pileup => {
        Iterator(pileup.locus)
      }, reference = TestUtil.makeReference(sc, Seq(("chr1", 0, "ATCGATCGA")))).collect.toSeq
    pileups should equal(Seq(1, 2, 3, 4, 5, 6, 7, 8))
  }

  sparkTest("test pileup flatmap two rdds; skip empty pileups") {
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

    val loci = DistributedUtil.pileupFlatMapTwoRDDs[Long](
      reads1,
      reads2,
      DistributedUtil.partitionLociUniformly(1, LociSet.parse("chr0:0-1000,chr1:1-500,chr2:10-20").result),
      true, // skip empty pileups
      (pileup1, pileup2) => (Iterator(pileup1.locus)),
      reference = TestUtil.makeReference(sc, Seq(("chr1", 0, "ATCGATCGA")))).collect
    loci.toSeq should equal(Seq(1, 2, 3, 4, 5, 6, 7, 8, 99, 100, 101, 102, 103, 104, 105, 106, 107))
  }

  sparkTest("test pileup flatmap multiple rdds; skip empty pileups") {
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

    val resultPlain = DistributedUtil.pileupFlatMapMultipleRDDs[Seq[Seq[String]]](
      Seq(reads1, reads2, reads3),
      DistributedUtil.partitionLociUniformly(1, LociSet.parse("chr1:1-500,chr2:10-20").result),
      true, // skip empty pileups
      pileups => Iterator(pileups.map(_.elements.map(p => Bases.basesToString(p.sequencedBases)))),
      reference = TestUtil.makeReference(sc, Seq(("chr1", 0, "ATCGATCGA")))).collect.map(_.toList)

    val resultParallelized = DistributedUtil.pileupFlatMapMultipleRDDs[Seq[Seq[String]]](
      Seq(reads1, reads2, reads3),
      DistributedUtil.partitionLociUniformly(800, LociSet.parse("chr0:0-100,chr1:1-500,chr2:10-20").result),
      true, // skip empty pileups
      pileups => Iterator(pileups.map(_.elements.map(p => Bases.basesToString(p.sequencedBases)))),
      reference = TestUtil.makeReference(sc, Seq(("chr1", 0, "ATCGATCGA")))).collect.map(_.toList)

    val resultWithEmpty = DistributedUtil.pileupFlatMapMultipleRDDs[Seq[Seq[String]]](
      Seq(reads1, reads2, reads3),
      DistributedUtil.partitionLociUniformly(5, LociSet.parse("chr1:1-500,chr2:10-20").result),
      false, // don't skip empty pileups
      pileups => Iterator(pileups.map(_.elements.map(p => Bases.basesToString(p.sequencedBases)))),
      reference = TestUtil.makeReference(
        sc, Seq(("chr1", 0, "ATCGATCGA"), ("chr2", 0, "")))).collect.map(_.toList)

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

  sparkTest("test pileup flatmap parallelism 5; create pileup elements") {

    val reads = sc.parallelize(Seq(
      TestUtil.makeRead("TCGATCGA", "8M", 1),
      TestUtil.makeRead("TCGATCGA", "8M", 1),
      TestUtil.makeRead("TCGATCGA", "8M", 1)))

    val pileups = DistributedUtil.pileupFlatMap[PileupElement](
      reads,
      DistributedUtil.partitionLociUniformly(5, LociSet.parse("chr1:1-9").result),
      false, // don't skip empty pileups
      pileup => pileup.elements.toIterator,
      reference = TestUtil.makeReference(sc, Seq(("chr1", 1, "TCGATCGA")))).collect()

    pileups.length should be(24)
    pileups.forall(_.isMatch) should be(true)
  }

  sparkTest("test two-rdd pileup flatmap; create pileup elements") {
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

    val elements = DistributedUtil.pileupFlatMapTwoRDDs[PileupElement](
      reads1,
      reads2,
      DistributedUtil.partitionLociUniformly(1000, LociSet.parse("chr1:1-500").result),
      false, // don't skip empty pileups
      (pileup1, pileup2) => (pileup1.elements ++ pileup2.elements).toIterator,
      reference = TestUtil.makeReference(sc, Seq(("chr1", 0, "ATCGATCGA" + "N" * 90 + "AGGGGGGGGGG")))).collect()

    elements.map(_.isMatch) should equal(List.fill(elements.length)(true))
    assertBases(
      elements.flatMap(_.sequencedBases).toSeq,
      "TTTTTTCCCCCCGGGGGGAAAAAATTTTTTCCCCCCGGGGGGAAAAAAAGGGGGGGGGGGGGGGGGGGGGGGGGG"
    )
  }

  sparkTest("test pileup flatmap parallelism 5; create pileup elements; with indel") {

    val reads = sc.parallelize(Seq(
      TestUtil.makeRead("TCGATCGA", "8M", 1),
      TestUtil.makeRead("TCGATCGA", "8M", 1),
      TestUtil.makeRead("TCGACCCTCGA", "4M3I4M", 1)))

    val pileups = DistributedUtil.pileupFlatMap[PileupElement](
      reads,
      DistributedUtil.partitionLociUniformly(5, LociSet.parse("chr1:1-12").result),
      false, // don't skip empty pileups
      pileup => pileup.elements.toIterator,
      reference = TestUtil.makeReference(sc, Seq(("chr1", 0, "ATCGATCGA")))).collect()

    pileups.length should be(24)
    val insertionPileups = pileups.filter(_.isInsertion)
    insertionPileups.size should be(1)
  }

  sparkTest("test pileup flatmap parallelism 0; thresholdvariant caller; no variant") {

    val reads = sc.parallelize(Seq(
      TestUtil.makeRead("TCGATCGA", "8M", 1),
      TestUtil.makeRead("TCGATCGA", "8M", 1),
      TestUtil.makeRead("TCGATCGA", "8M", 1)))

    val genotypes = DistributedUtil.pileupFlatMap[Genotype](
      reads,
      DistributedUtil.partitionLociUniformly(reads.partitions.length, LociSet.parse("chr1:1-100").result),
      false, // don't skip empty pileups
      pileup => GermlineThreshold.Caller.callVariantsAtLocus(pileup, 0, false, false).iterator,
      reference = TestUtil.makeReference(sc, Seq(("chr1", 0, "ATCGATCGA")))).collect()

    genotypes.length should be(0)
  }

  sparkTest("test pileup flatmap parallelism 3; thresholdvariant caller; no variant") {

    val reads = sc.parallelize(Seq(
      TestUtil.makeRead("TCGATCGA", "8M", 1),
      TestUtil.makeRead("TCGATCGA", "8M", 1),
      TestUtil.makeRead("TCGATCGA", "8M", 1)))

    val genotypes = DistributedUtil.pileupFlatMap[Genotype](
      reads,
      DistributedUtil.partitionLociUniformly(3, LociSet.parse("chr1:1-100").result),
      false, // don't skip empty pileups
      pileup => GermlineThreshold.Caller.callVariantsAtLocus(pileup, 0, false, false).iterator,
      reference = TestUtil.makeReference(sc, Seq(("chr1", 0, "ATCGATCGA")))).collect()

    genotypes.length should be(0)
  }

  sparkTest("test pileup flatmap parallelism 3; thresholdvariant caller; one het variant") {

    val reads = sc.parallelize(Seq(
      TestUtil.makeRead("TCGATCGA", "8M", 1),
      TestUtil.makeRead("TCGGTCGA", "8M", 1),
      TestUtil.makeRead("TCGGTCGA", "8M", 1)))

    val genotypes = DistributedUtil.pileupFlatMap[Genotype](
      reads,
      DistributedUtil.partitionLociUniformly(3, LociSet.parse("chr1:1-100").result),
      false, // don't skip empty pileups
      pileup => GermlineThreshold.Caller.callVariantsAtLocus(pileup, 0, false, false).iterator,
      reference = TestUtil.makeReference(sc, Seq(("chr1", 0, "ATCGATCGA")))).collect()

    genotypes.length should be(1)
    genotypes.head.getVariant.getStart should be(4)
    genotypes.head.getVariant.getReferenceAllele should be("A")
    genotypes.head.getVariant.getAlternateAllele should be("G")

    genotypes.head.getAlleles.toList should be(List(GenotypeAllele.Ref, GenotypeAllele.Alt))
  }

  sparkTest("test pileup flatmap parallelism 3; thresholdvariant caller; no reference bases observed") {

    val reads = sc.parallelize(Seq(
      TestUtil.makeRead("CCGATCGA", "8M", 1),
      TestUtil.makeRead("CCGATCGA", "8M", 1),
      TestUtil.makeRead("CCGATCGA", "8M", 1)))

    val genotypes = DistributedUtil.pileupFlatMap[Genotype](
      reads,
      DistributedUtil.partitionLociUniformly(3, LociSet.parse("chr1:1-100").result),
      false, // don't skip empty pileups
      pileup => GermlineThreshold.Caller.callVariantsAtLocus(pileup, 0, false, false).iterator,
      reference = TestUtil.makeReference(sc, Seq(("chr1", 0, "ATCGATCGA")))).collect()

    genotypes.length should be(1)
    genotypes.head.getVariant.getStart should be(1)
    genotypes.head.getVariant.getReferenceAllele.toString should be("T")
    genotypes.head.getVariant.getAlternateAllele.toString should be("C")
    genotypes.head.getAlleles.toList should be(List(GenotypeAllele.Alt, GenotypeAllele.Alt))
  }

  sparkTest("test window fold parallelism 5; average read depth") {

    // 4 overlapping reads starting at loci = 0
    //     0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6
    // r1: T C G A T C G G
    // r2:   C C C C C C C C
    // r3:         T C G A T C G A
    // r4:                   G G G G G G G
    // At pos = 0, the depth is 1
    // At pos = 1, 2, 3,  the depth is 2
    // At pos = 4 through 7, the depth is 3
    // At pos = 8 - 11 through 11, the depth is 2

    val reads = sc.parallelize(Seq(
      TestUtil.makeRead("TCGATCGGC", "8M", 0),
      TestUtil.makeRead("CCCCCCCC", "8M", 1),
      TestUtil.makeRead("TCGATCGA", "8M", 4),
      TestUtil.makeRead("GGGGGGG", "7M", 9)))

    val counts = DistributedUtil.windowFoldLoci(
      Seq(reads),
      // Split loci in 5 partitions - we will compute an aggregate value per partition
      DistributedUtil.partitionLociUniformly(5, LociSet.parse("chr1:0-20").result),
      skipEmpty = false,
      halfWindowSize = 0,
      initialValue = (0L, 0L),
      // averageDepth is represented as fraction tuple (numerator, denominator) == (totalDepth, totalLoci)
      (averageDepth: (Long, Long), windows: Seq[SlidingWindow[MappedRead]]) => {
        val currentDepth = windows.map(w => w.currentRegions().count(_.overlapsLocus(w.currentLocus))).sum
        (averageDepth._1 + currentDepth, averageDepth._2 + 1)
      }
    ).collect()

    counts.size should be(5)
    counts(0) should be(7, 4) // average depth between [0, 3] is 7/4 = 2.75
    counts(1) should be(12, 4) // average depth between [4, 7] is 12/4 = 3
    counts(2) should be(8, 4) // average depth between [8, 11] is 8/4 = 2
    counts(3) should be(4, 4) // average depth between [12, 15] is 4/4 = 2
    counts(4) should be(0, 4) // average depth between [16, 19] is 0/4 = 0
  }
}
