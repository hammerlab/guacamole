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

package org.bdgenomics.guacamole

import org.scalatest.matchers.ShouldMatchers
import org.bdgenomics.adam.avro.{ ADAMGenotypeAllele, ADAMGenotype }
import org.bdgenomics.guacamole.callers.ThresholdVariantCaller
import scala.collection.JavaConversions._
import org.bdgenomics.guacamole.pileup.{ PileupElement, Pileup }

class DistributedUtilSuite extends TestUtil.SparkFunSuite with ShouldMatchers {

  test("partitionLociUniformly") {
    val set = LociSet.parse("chr21:100-200,chr20:0-10,chr20:8-15,chr20:100-121,empty:10-10")
    val result1 = DistributedUtil.partitionLociUniformly(1, set).asInverseMap
    result1(0) should equal(set)

    val result2 = DistributedUtil.partitionLociUniformly(2, set).asInverseMap
    result2(0).count should equal(set.count / 2)
    result2(1).count should equal(set.count / 2)
    result2(0) should not equal (result2(1))
    result2(0).union(result2(1)) should equal(set)

    val result3 = DistributedUtil.partitionLociUniformly(4, LociSet.parse("chrM:0-16571"))
    result3.toString should equal("chrM:0-4143=0,chrM:4143-8286=1,chrM:8286-12428=2,chrM:12428-16571=3")

    val result4 = DistributedUtil.partitionLociUniformly(100, LociSet.parse("chrM:1000-1100"))
    val expectedBuilder4 = LociMap.newBuilder[Long]
    for (i <- 0 until 100) {
      expectedBuilder4.put("chrM", i + 1000, i + 1001, i)
    }
    result4 should equal(expectedBuilder4.result)

    val result5 = DistributedUtil.partitionLociUniformly(3, LociSet.parse("chrM:0-10"))
    result5.toString should equal("chrM:0-3=0,chrM:3-7=1,chrM:7-10=2")

    val result6 = DistributedUtil.partitionLociUniformly(4, LociSet.parse("chrM:0-3"))
    result6.toString should equal("chrM:0-1=0,chrM:1-2=1,chrM:2-3=2")

    val result7 = DistributedUtil.partitionLociUniformly(4, LociSet.parse("empty:10-10"))
    result7.toString should equal("")
  }

  test("partitionLociUniformly performance") {
    // These two calls should not take a noticeable amount of time.
    val bigSet = LociSet.parse("chr21:0-3000000000")
    DistributedUtil.partitionLociUniformly(2000, bigSet).asInverseMap

    val giantSet = LociSet.parse("1:0-249250621,10:0-135534747,11:0-135006516,12:0-133851895,13:0-115169878,14:0-107349540,15:0-102531392,16:0-90354753,17:0-81195210,18:0-78077248,19:0-59128983,2:0-243199373,20:0-63025520,21:0-48129895,22:0-51304566,3:0-198022430,4:0-191154276,5:0-180915260,6:0-171115067,7:0-159138663,8:0-146364022,9:0-141213431,GL000191.1:0-106433,GL000192.1:0-547496,GL000193.1:0-189789,GL000194.1:0-191469,GL000195.1:0-182896,GL000196.1:0-38914,GL000197.1:0-37175,GL000198.1:0-90085,GL000199.1:0-169874,GL000200.1:0-187035,GL000201.1:0-36148,GL000202.1:0-40103,GL000203.1:0-37498,GL000204.1:0-81310,GL000205.1:0-174588,GL000206.1:0-41001,GL000207.1:0-4262,GL000208.1:0-92689,GL000209.1:0-159169,GL000210.1:0-27682,GL000211.1:0-166566,GL000212.1:0-186858,GL000213.1:0-164239,GL000214.1:0-137718,GL000215.1:0-172545,GL000216.1:0-172294,GL000217.1:0-172149,GL000218.1:0-161147,GL000219.1:0-179198,GL000220.1:0-161802,GL000221.1:0-155397,GL000222.1:0-186861,GL000223.1:0-180455,GL000224.1:0-179693,GL000225.1:0-211173,GL000226.1:0-15008,GL000227.1:0-128374,GL000228.1:0-129120,GL000229.1:0-19913,GL000230.1:0-43691,GL000231.1:0-27386,GL000232.1:0-40652,GL000233.1:0-45941,GL000234.1:0-40531,GL000235.1:0-34474,GL000236.1:0-41934,GL000237.1:0-45867,GL000238.1:0-39939,GL000239.1:0-33824,GL000240.1:0-41933,GL000241.1:0-42152,GL000242.1:0-43523,GL000243.1:0-43341,GL000244.1:0-39929,GL000245.1:0-36651,GL000246.1:0-38154,GL000247.1:0-36422,GL000248.1:0-39786,GL000249.1:0-38502,MT:0-16569,NC_007605:0-171823,X:0-155270560,Y:0-59373566,hs37d5:0-35477943")
    DistributedUtil.partitionLociUniformly(2000, giantSet).asInverseMap

  }

  sparkTest("test pileup flatmap parallelism 0; create pileups") {

    val reads = sc.parallelize(Seq(
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1)))

    val pileups = DistributedUtil.pileupFlatMap[Pileup](
      reads,
      LociSet.parse("chr1:1-9"),
      0,
      pileup => Seq(pileup).iterator).collect()

    pileups.length should be(8)
    val firstPileup = pileups.head
    firstPileup.locus should be(1L)
    firstPileup.referenceBase should be('T')

    firstPileup.elements.forall(_.readPosition == 0L) should be(true)
    firstPileup.elements.forall(_.isMatch) should be(true)

    pileups.forall(p => p.head.isMatch) should be(true)

  }

  sparkTest("test pileup flatmap parallelism 5; create pileups") {

    val reads = sc.parallelize(Seq(
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1)))

    val pileups = DistributedUtil.pileupFlatMap[Pileup](
      reads,
      LociSet.parse("chr1:1-9"),
      5,
      pileup => Seq(pileup).iterator).collect()

    val firstPileup = pileups.head
    firstPileup.locus should be(1L)
    firstPileup.referenceBase should be('T')

    pileups.forall(p => p.head.isMatch) should be(true)

  }

  sparkTest("test pileup flatmap parallelism 5; create pileup elements") {

    val reads = sc.parallelize(Seq(
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1)))

    val pileups = DistributedUtil.pileupFlatMap[PileupElement](
      reads,
      LociSet.parse("chr1:1-9"),
      5,
      pileup => pileup.elements.toIterator).collect()

    pileups.length should be(24)

    pileups.forall(_.isMatch) should be(true)

  }

  sparkTest("test pileup flatmap parallelism 5; create pileup elements; with indel") {

    val reads = sc.parallelize(Seq(
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeRead("TCGACCCTCGA", "4M3I4M", "8", 1)))

    val pileups = DistributedUtil.pileupFlatMap[PileupElement](
      reads,
      LociSet.parse("chr1:1-12"),
      5,
      pileup => pileup.elements.toIterator).collect()

    pileups.length should be(24)
    val insertionPileups = pileups.filter(_.isInsertion)
    insertionPileups.size should be(1)

  }

  sparkTest("test pileup flatmap parallelism 0; thresholdvariant caller; no variant") {

    val reads = sc.parallelize(Seq(
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1)))

    val genotypes = DistributedUtil.pileupFlatMap[ADAMGenotype](
      reads,
      Common.getLociFromAllContigs(reads),
      0,
      pileup => ThresholdVariantCaller.callVariantsAtLocus(pileup, 0, false, false).iterator).collect()

    genotypes.length should be(0)

  }

  sparkTest("test pileup flatmap parallelism 3; thresholdvariant caller; no variant") {

    val reads = sc.parallelize(Seq(
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1)))

    val genotypes = DistributedUtil.pileupFlatMap[ADAMGenotype](
      reads,
      Common.getLociFromAllContigs(reads),
      3,
      pileup => ThresholdVariantCaller.callVariantsAtLocus(pileup, 0, false, false).iterator).collect()

    genotypes.length should be(0)

  }

  sparkTest("test pileup flatmap parallelism 3; thresholdvariant caller; one het variant") {

    val reads = sc.parallelize(Seq(
      TestUtil.makeRead("TCGATCGA", "8M", "8", 1),
      TestUtil.makeRead("TCGGTCGA", "8M", "8", 1),
      TestUtil.makeRead("TCGGTCGA", "8M", "8", 1)))

    val genotypes = DistributedUtil.pileupFlatMap[ADAMGenotype](
      reads,
      Common.getLociFromAllContigs(reads),
      3,
      pileup => ThresholdVariantCaller.callVariantsAtLocus(pileup, 0, false, false).iterator).collect()

    genotypes.length should be(1)

    genotypes.head.alleles.toList should be(List(ADAMGenotypeAllele.Ref, ADAMGenotypeAllele.Alt))

  }
}