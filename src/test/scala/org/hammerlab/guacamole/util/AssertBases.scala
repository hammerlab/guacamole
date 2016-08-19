package org.hammerlab.guacamole.util

import org.scalatest.Matchers

object AssertBases extends Matchers {
  def apply(bases1: Iterable[Byte], bases2: String) = Bases.basesToString(bases1) should equal(bases2)
}
