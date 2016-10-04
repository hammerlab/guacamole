package org.hammerlab.guacamole.util

import org.scalatest.Matchers
import org.hammerlab.guacamole.util.Bases.basesToString

object AssertBases extends Matchers {
  def apply(bases1: Iterable[Byte], bases2: String) = basesToString(bases1) should equal(bases2)
}
