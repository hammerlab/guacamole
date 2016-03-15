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

import org.scalatest.FunSuite

import scala.math.log
import scala.util.Random
import org.scalactic.TolerantNumerics

class LogUtilsSuite extends FunSuite {
  implicit val doubleEq = TolerantNumerics.tolerantDoubleEquality(1e-6f)

  test("test our nifty log summer") {

    val sumLogs = LogUtils.sumLogProbabilities(Array(0.5, 0.25, 0.125, 0.1, 0.025).map(log(_)))
    assert(sumLogs === 0.0d)
  }

  test("can we compute the sum of logs correctly?") {
    val rv = new Random(124235346L)

    (0 to 1000).foreach(i => {
      val p = rv.nextDouble()
      val q = rv.nextDouble()

      val logPplusQ = LogUtils.logSum(log(p), log(q))

      assert(logPplusQ === log(p + q))
    })
  }

  test("can we compute the additive inverse of logs correctly?") {
    val rv = new Random(223344556677L)

    (0 to 1000).foreach(i => {
      val p = rv.nextDouble()

      val log1mP = LogUtils.logAdditiveInverse(log(p))

      assert(log1mP === log(1.0 - p))
    })
  }
}
