package org.hammerlab.guacamole.commands

import org.hammerlab.guacamole.util.GuacFunSuite
import org.scalatest.Matchers

class StructuralVariantCallerSuite extends GuacFunSuite with Matchers {
  test("coalesce intervals") {
    val nums = Array(0L, 10L, 20L, 30L,
                     50L,
                     70L, 80L,
                     100L, 110L, 120L,
                     150L)
    val ranges = StructuralVariant.Caller.coalesceAdjacent(nums, 10)
    assert(ranges.toList === List((0L, 30L), (50L, 50L), (70L, 80L), (100L, 120L), (150L, 150L)))
  }
}
