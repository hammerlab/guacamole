package org.hammerlab.guacamole.filters

import org.apache.commons.math3.util.ArithmeticUtils

object FishersExactTest {

  /** Fisher's exact test, returned as a probability. */
  def apply(totalA: Int, totalB: Int, conditionA: Int, conditionB: Int): Double = {
    math.exp(ArithmeticUtils.binomialCoefficientLog(totalA, conditionA) +
      ArithmeticUtils.binomialCoefficientLog(totalB, conditionB) -
      ArithmeticUtils.binomialCoefficientLog(totalA + totalB, conditionA + conditionB))
  }

  /** Fisher's exact test, returned as -1 * log base 10 probability (i.e. a positive number). */
  def asLog10(totalA: Int, totalB: Int, conditionA: Int, conditionB: Int): Double = {
    (ArithmeticUtils.binomialCoefficientLog(totalA, conditionA) +
      ArithmeticUtils.binomialCoefficientLog(totalB, conditionB) -
      ArithmeticUtils.binomialCoefficientLog(totalA + totalB, conditionA + conditionB)
      / Math.log(10))
  }
}