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

import breeze.stats.distributions.Binomial
import org.scalatest.FunSuite

import scala.math.log
import scala.util.Random
import org.scalactic.TolerantNumerics

class LogBinomialSuite extends FunSuite {
  implicit val doubleEq = TolerantNumerics.tolerantDoubleEquality(1e-6f)

  test("calculating log binomials is approximately correct") {
    val rv = new Random(45567890123L)

    (0 to 1000).foreach(i => {
      val n = rv.nextInt(100) + 1
      val p = rv.nextDouble()

      val binomialDist = Binomial(n, p)
      val binomialValues = LogBinomial.calculateLogProbabilities(log(p), n)

      (0 to n).foreach(j => {
        assert(binomialDist.logProbabilityOf(j) === binomialValues(j))
      })
    })
  }
}
