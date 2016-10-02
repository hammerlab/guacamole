package org.hammerlab.guacamole.util

import org.scalatest.{FunSuite, Matchers}
import org.hammerlab.guacamole.util.Bases.{basesToString, reverseComplement, stringToBases}

class BasesSuite extends FunSuite with Matchers {
  test("string conversions, reverse complement") {
    basesToString(reverseComplement(stringToBases("AGGTCA"))) should equal("TGACCT")
  }
}
