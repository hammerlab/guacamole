package org.hammerlab.guacamole.util

import org.bdgenomics.adam.util.{PhredUtils => BDGPhredUtils}

object PhredUtils {
  // Helps avoid singularities in phred/log computations.
  val one = 1 - 1e-16

  def successProbabilityToPhred(probability: Double): Int =
    BDGPhredUtils.successProbabilityToPhred(
      math.min(
        one,
        probability
      )
    )
}
