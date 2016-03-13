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

import scala.math.{ exp, expm1, log, log1p }

object LogUtils extends Serializable {

  /**
   * Given an array of log probabilities, computes the log of the sum of the
   * probabilities. E.g., computes:
   *
   * log(p0 + ... + pn) = sumLogProbabilities(Array(log(p0), ..., log(pn)))
   *                    ~ log(Array.map(exp(_)).sum)
   *
   * @param pArray Array of log probabilities to sum.
   * @return The log of the sum of the probabilities.
   *
   * @note This function is not numerically sensitive to the order of the
   *       values in the array. The array is copied and sorted before the
   *       sum is applied.
   * @see logSum
   */
  def sumLogProbabilities(pArray: Array[Double]): Double = {
    assert(pArray.length > 0)

    // first, sort the array
    val sortP = pArray.toSeq.sortWith(_ > _)

    def sumFunction(logP: Double,
                    logIter: Iterator[Double]): Double = {
      if (!logIter.hasNext) {
        logP
      } else {
        sumFunction(internalLogSum(logP, logIter.next), logIter)
      }
    }

    sumFunction(sortP.head, sortP.toIterator.drop(1))
  }

  /**
   * This is a nifty little trick for summing logs. Not sure exactly where it's
   * originally from, but apparently Durbin et al '98 features it. evidently:
   * log(p + q) = log(p(1 + q/p)
   *            = log(p) + log(1 + exp(log(q/p)))
   *            = log(p) + log(1 + exp(log(q) - log(p)))
   *
   * @param logP The log of the first probability.
   * @param logQ The log of the second probability.
   * @return Returns the log of the sum of the two probabilities.
   *
   * @note For reasons of numerical precision, logP should be greater than logQ.
   */
  private def internalLogSum(logP: Double,
                             logQ: Double): Double = {
    logP + log1p(exp(logQ - logP))
  }

  /**
   * Given two log scaled values, compute the log of the sum of the values. E.g.,
   * log(p + q) = logSum(log(p), log(q))
   *
   * @param logP The log of the first value.
   * @param logQ The log of the second value.
   * @return The log of the sums of the first value and the second value.
   *
   * @note This function is not sensitive to the relative value of P and Q. We check
   *       inside of the function to determine whether P or Q is larger.
   */
  def logSum(logP: Double,
             logQ: Double): Double = {
    if (logP >= logQ && logP != 0.0) {
      internalLogSum(logP, logQ)
    } else {
      internalLogSum(logQ, logP)
    }
  }

  /**
   * This is a nifty little trick for subtracting logs. Not sure exactly where it's
   * originally from, but apparently Durbin et al '98 features it. evidently:
   * log(p - q) = log(p(1 - q/p)
   *            = log(p) + log(1 - exp(log(q/p)))
   *            = log(p) + log(1 - exp(log(q) - log(p)))
   *            = log(p) + log(-(exp(log(q) - log(p)) - 1))
   *
   * @param logP The log of the first probability.
   * @param logQ The log of the second probability.
   * @return Returns the log of the second probability subtracted from the first probability.
   *
   * @note For reasons of numerical precision, logP should be greater than logQ.
   */
  private def internalLogSubtract(logP: Double,
                                  logQ: Double): Double = {
    logP + log(-expm1(logQ - logP))
  }

  /**
   * Computes the log of the additive inverse of a log value. E.g., given log p, we compute
   * log(1 - p).
   *
   * @param logP The log of the value.
   * @return The log of 1 minus the value.
   *
   * @note From the perspective of numerical precision, we expect logP to be less than 0.
   *       This implies that P is expected to be between 0 and 1, non-inclusive.
   */
  def logAdditiveInverse(logP: Double): Double = {
    internalLogSubtract(0.0, logP)
  }
}
