package org.hammerlab.guacamole.filters

import org.apache.commons.math3.util.ArithmeticUtils.binomialCoefficientLog

object FishersExactTest {

  /** Fisher's exact test, returned as a probability. */
  def apply(totalA: Int, totalB: Int, conditionA: Int, conditionB: Int): Double =
    math.exp(asLog(totalA, totalB, conditionA, conditionB))

  /** Fisher's exact test, returned as -1 * log base 10 probability (i.e. a positive number). */
  def asLog10(totalA: Int, totalB: Int, conditionA: Int, conditionB: Int): Double =
    asLog(totalA, totalB, conditionA, conditionB) / Math.log(10)

  /** Fisher's exact test, returned as -1 * natural-log probability (i.e. a positive number). */
  def asLog(totalA: Int, totalB: Int, conditionA: Int, conditionB: Int): Double =
    binomialCoefficientLog(totalA, conditionA) +
      binomialCoefficientLog(totalB, conditionB) -
      binomialCoefficientLog(totalA + totalB, conditionA + conditionB)
}
