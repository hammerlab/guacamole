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

import scala.math.log

object LogBinomial extends Serializable {

  // TODO: for efficiency, we should cache these values and not recompute
  def logBinomial(n: Int,
                  k: Int): Double = {
    ((n - k + 1) to n).map(v => log(v.toDouble)).sum - (1 to k).map(v => log(v.toDouble)).sum
  }

  /**
   * For a binomial distribution with a given log success probability and
   * max number of events, returns the log probabilities associated with the
   * number of events between 0 and the max number of events.
   *
   * @param logP The log success probability of an event.
   * @param m The max number of events.
   * @return Returns an m + 1 length array with the log probability of each possible
   *         success count.
   */
  def calculateLogProbabilities(logP: Double,
                                m: Int): Array[Double] = {

    // take the additive inverse of P
    val log1mP = LogUtils.logAdditiveInverse(logP)

    // loop to calculate probabilities
    val pArray = new Array[Double](m + 1)
    var idx = 0

    while (idx <= m) {
      // if we special case idx and m == 0, we can save some compute
      if (idx == 0) {
        pArray(0) = m.toDouble * log1mP
      } else if (idx != m) {
        pArray(idx) = logBinomial(m, idx) + (idx * logP).toDouble + ((m - idx).toDouble * log1mP)
      } else {
        pArray(m) = m.toDouble * logP
      }
      idx += 1
    }

    // return probability array
    pArray
  }
}
