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

package org.hammerlab.guacamole.likelihood

import org.hammerlab.guacamole.likelihood.LikelihoodUtil._
import org.hammerlab.guacamole.pileup.Pileup
import org.hammerlab.guacamole.reads.MappedRead
import org.hammerlab.guacamole.util.{GuacFunSuite, TestUtil}
import org.hammerlab.guacamole.variants.Genotype
import org.scalatest.prop.TableDrivenPropertyChecks

class LikelihoodSuite extends GuacFunSuite with TableDrivenPropertyChecks {

  def testLikelihoods(actualLikelihoods: Seq[(Genotype, Double)],
                      expectedLikelihoods: ((Char, Char), Double)*): Unit =
    testLikelihoods(actualLikelihoods, expectedLikelihoods.toList.map(p => makeGenotype(p._1) -> p._2).toMap)

  def testLikelihoods(actualLikelihoods: Seq[(Genotype, Double)],
                      expectedLikelihoods: Map[Genotype, Double],
                      acceptableError: Double = 1e-12): Unit = {
    actualLikelihoods.size should equal(expectedLikelihoods.size)
    val actualLikelihoodsMap = actualLikelihoods.toMap
    forAll(Table("genotype", expectedLikelihoods.toList: _*)) {
      l => TestUtil.assertAlmostEqual(actualLikelihoodsMap(l._1), l._2, acceptableError)
    }
  }

  def testGenotypeLikelihoods(reads: Seq[MappedRead], genotypesMap: ((Char, Char), Double)*): Unit = {
    val referenceContigSequence = referenceBroadcast(sc).getContig("chr1")
    val pileup = Pileup(reads, reads(0).contigName, 1, referenceContigSequence)
    forAll(Table("genotype", genotypesMap: _*)) { pair =>
      TestUtil.assertAlmostEqual(
        Likelihood.likelihoodOfGenotype(
          pileup.elements,
          makeGenotype(pair._1), // genotype
          Likelihood.probabilityCorrectIgnoringAlignment),
        pair._2
      )
    }
  }

  test("all reads ref") {
    testGenotypeLikelihoods(
      Seq(refRead(30), refRead(40), refRead(30)),
      ('C', 'C') -> (1 - errorPhred30) * (1 - errorPhred40) * (1 - errorPhred30),
      ('C', 'A') -> 1.0 / 8,
      ('A', 'C') -> 1.0 / 8,
      ('A', 'A') -> errorPhred30 * errorPhred40 * errorPhred30,
      ('A', 'T') -> errorPhred30 * errorPhred40 * errorPhred30
    )
  }

  test("two ref, one alt") {
    testGenotypeLikelihoods(
      Seq(refRead(30), refRead(40), altRead(30)),
      ('C', 'C') -> (1 - errorPhred30) * (1 - errorPhred40) * errorPhred30,
      ('C', 'A') -> 1.0 / 8,
      ('A', 'C') -> 1.0 / 8,
      ('A', 'A') -> errorPhred30 * errorPhred40 * (1 - errorPhred30),
      ('A', 'T') -> errorPhred30 * errorPhred40 * 1 / 2,
      ('T', 'T') -> errorPhred30 * errorPhred40 * errorPhred30
    )
  }

  test("one ref, two alt") {
    testGenotypeLikelihoods(
      Seq(refRead(30), altRead(40), altRead(30)),
      ('C', 'C') -> (1 - errorPhred30) * errorPhred40 * errorPhred30,
      ('C', 'A') -> 1.0 / 8,
      ('A', 'C') -> 1.0 / 8,
      ('A', 'A') -> errorPhred30 * (1 - errorPhred40) * (1 - errorPhred30),
      ('A', 'T') -> errorPhred30 * 1 / 2 * 1 / 2,
      ('T', 'T') -> errorPhred30 * errorPhred40 * errorPhred30
    )
  }

  test("all reads alt") {
    testGenotypeLikelihoods(
      Seq(altRead(30), altRead(40), altRead(30)),
      ('C', 'C') -> errorPhred30 * errorPhred40 * errorPhred30,
      ('C', 'A') -> 1.0 / 8,
      ('A', 'C') -> 1.0 / 8,
      ('A', 'A') -> (1 - errorPhred30) * (1 - errorPhred40) * (1 - errorPhred30),
      ('A', 'T') -> 1.0 / 8,
      ('T', 'T') -> errorPhred30 * errorPhred40 * errorPhred30
    )
  }

  test("score genotype for single sample; all bases ref") {
    val referenceContigSequence = referenceBroadcast(sc).getContig("chr1")

    val reads = Seq(
      refRead(30),
      refRead(40),
      refRead(30)
    )

    val pileup = Pileup(reads, "chr1", 1, referenceContigSequence)

    testLikelihoods(
      Likelihood.likelihoodsOfAllPossibleGenotypesFromPileup(pileup, Likelihood.probabilityCorrectIgnoringAlignment),
      ('C', 'C') -> (1 - errorPhred30) * (1 - errorPhred40) * (1 - errorPhred30)
    )
  }

  test("score genotype for single sample; mix of ref/non-ref bases") {
    val referenceContigSequence = referenceBroadcast(sc).getContig("chr1")

    val reads = Seq(
      refRead(30),
      refRead(40),
      altRead(30)
    )

    val pileup = Pileup(reads, "chr1", 1, referenceContigSequence)

    testLikelihoods(
      Likelihood.likelihoodsOfAllPossibleGenotypesFromPileup(pileup, Likelihood.probabilityCorrectIgnoringAlignment),
      ('C', 'C') -> (1 - errorPhred30) * (1 - errorPhred40) * errorPhred30,
      ('A', 'C') -> 1 / 8.0,
      ('A', 'A') -> errorPhred30 * errorPhred40 * (1 - errorPhred30)
    )
  }

  test("score genotype for single sample; all bases non-ref") {
    val referenceContigSequence = referenceBroadcast(sc).getContig("chr1")

    val reads = Seq(
      altRead(30),
      altRead(40),
      altRead(30)
    )

    val pileup = Pileup(reads, "chr1", 1, referenceContigSequence)

    testLikelihoods(
      Likelihood.likelihoodsOfAllPossibleGenotypesFromPileup(pileup, Likelihood.probabilityCorrectIgnoringAlignment),
      ('A', 'A') -> (1 - errorPhred30) * (1 - errorPhred40) * (1 - errorPhred30)
    )
  }

  test("log score genotype for single sample; all bases ref") {
    val referenceContigSequence = referenceBroadcast(sc).getContig("chr1")

    val reads = Seq(
      refRead(30),
      refRead(40),
      refRead(30)
    )

    val pileup = Pileup(reads, "chr1", 1, referenceContigSequence)

    testLikelihoods(
      Likelihood.likelihoodsOfAllPossibleGenotypesFromPileup(
        pileup,
        Likelihood.probabilityCorrectIgnoringAlignment,
        logSpace = true),
      ('C', 'C') -> (math.log(1 - errorPhred30) + math.log(1 - errorPhred40) + math.log(1 - errorPhred30))
    )
  }

  test("log score genotype for single sample; mix of ref/non-ref bases") {
    val referenceContigSequence = referenceBroadcast(sc).getContig("chr1")

    val reads = Seq(
      refRead(30),
      refRead(40),
      altRead(30)
    )

    val pileup = Pileup(reads, "chr1", 1, referenceContigSequence)

    testLikelihoods(
      Likelihood.likelihoodsOfAllPossibleGenotypesFromPileup(
        pileup,
        Likelihood.probabilityCorrectIgnoringAlignment,
        logSpace = true),
      ('C', 'C') -> (math.log(1 - errorPhred30) + math.log(1 - errorPhred40) + math.log(errorPhred30)),
      ('A', 'C') -> math.log(1.0 / 8),
      ('A', 'A') -> (math.log(errorPhred30) + math.log(errorPhred40) + math.log(1 - errorPhred30))
    )
  }

  test("log score genotype for single sample; all bases non-ref") {
    val referenceContigSequence = referenceBroadcast(sc).getContig("chr1")

    val reads = Seq(
      altRead(30),
      altRead(40),
      altRead(30)
    )

    val pileup = Pileup(reads, "chr1", 1, referenceContigSequence)

    testLikelihoods(
      Likelihood.likelihoodsOfAllPossibleGenotypesFromPileup(
        pileup,
        Likelihood.probabilityCorrectIgnoringAlignment,
        logSpace = true),
      ('A', 'A') -> (math.log(1 - errorPhred30) + math.log(1 - errorPhred40) + math.log(1 - errorPhred30))
    )
  }
}
