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

package org.bdgenomics.guacamole.variants

import org.bdgenomics.guacamole.TestUtil.assertAlmostEqual
import org.bdgenomics.guacamole.pileup.Pileup
import org.bdgenomics.guacamole.reads.MappedRead
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{ Matchers, FunSuite }
import org.bdgenomics.guacamole.ReadsUtil._

class GenotypeSuite extends FunSuite with Matchers with TableDrivenPropertyChecks {

  def testGenotypeLikelihoods(reads: Seq[MappedRead], genotypesMap: (Genotype, Double)*): Unit = {
    val pileup = Pileup(reads, 1)
    forAll(Table("genotype", genotypesMap: _*)) { l =>
      assertAlmostEqual(
        l._1.likelihoodOfReads(pileup.elements, includeAlignmentLikelihood = false),
        l._2
      )
    }
  }

  test("all reads ref") {
    testGenotypeLikelihoods(
      Seq(refRead(30), refRead(40), refRead(30)),
      makeGenotype("C", "C") -> (1 - errorPhred30) * (1 - errorPhred40) * (1 - errorPhred30),
      makeGenotype("C", "A") -> 1.0 / 8,
      makeGenotype("A", "C") -> 1.0 / 8,
      makeGenotype("A", "A") -> errorPhred30 * errorPhred40 * errorPhred30,
      makeGenotype("A", "T") -> errorPhred30 * errorPhred40 * errorPhred30
    )
  }

  test("two ref, one alt") {
    testGenotypeLikelihoods(
      Seq(refRead(30), refRead(40), altRead(30)),
      makeGenotype("C", "C") -> (1 - errorPhred30) * (1 - errorPhred40) * errorPhred30,
      makeGenotype("C", "A") -> 1.0 / 8,
      makeGenotype("A", "C") -> 1.0 / 8,
      makeGenotype("A", "A") -> errorPhred30 * errorPhred40 * (1 - errorPhred30),
      makeGenotype("A", "T") -> errorPhred30 * errorPhred40 * 1 / 2,
      makeGenotype("T", "T") -> errorPhred30 * errorPhred40 * errorPhred30
    )
  }

  test("one ref, two alt") {
    testGenotypeLikelihoods(
      Seq(refRead(30), altRead(40), altRead(30)),
      makeGenotype("C", "C") -> (1 - errorPhred30) * errorPhred40 * errorPhred30,
      makeGenotype("C", "A") -> 1.0 / 8,
      makeGenotype("A", "C") -> 1.0 / 8,
      makeGenotype("A", "A") -> errorPhred30 * (1 - errorPhred40) * (1 - errorPhred30),
      makeGenotype("A", "T") -> errorPhred30 * 1 / 2 * 1 / 2,
      makeGenotype("T", "T") -> errorPhred30 * errorPhred40 * errorPhred30
    )
  }

  test("all reads alt") {
    testGenotypeLikelihoods(
      Seq(altRead(30), altRead(40), altRead(30)),
      makeGenotype("C", "C") -> errorPhred30 * errorPhred40 * errorPhred30,
      makeGenotype("C", "A") -> 1.0 / 8,
      makeGenotype("A", "C") -> 1.0 / 8,
      makeGenotype("A", "A") -> (1 - errorPhred30) * (1 - errorPhred40) * (1 - errorPhred30),
      makeGenotype("A", "T") -> 1.0 / 8,
      makeGenotype("T", "T") -> errorPhred30 * errorPhred40 * errorPhred30
    )
  }
}
