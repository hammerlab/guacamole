package org.bdgenomics.guacamole.filters

import org.apache.commons.math3.util.ArithmeticUtils

object FishersExactTest {

  def apply(totalA: Int, totalB: Int, conditionA: Int, conditionB: Int): Double = {
    math.exp(ArithmeticUtils.binomialCoefficientLog(totalA, conditionA) +
      ArithmeticUtils.binomialCoefficientLog(totalB, conditionB) -
      ArithmeticUtils.binomialCoefficientLog(totalA + totalB, conditionA + conditionB))
  }

}