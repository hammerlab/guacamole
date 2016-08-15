package org.hammerlab.guacamole.util

import org.scalatest.{FunSuite, Matchers}

class BasesSuite extends FunSuite with Matchers {

  test("string conversions, reverse complement") {
    Bases.basesToString(Bases.reverseComplement(Bases.stringToBases("AGGTCA"))) should equal("TGACCT")
  }

}
