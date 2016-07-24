package org.hammerlab.guacamole.reads

import org.hammerlab.guacamole.reference.{ContigName, ReferenceRegion}

case class TestRegion(contigName: ContigName, start: Long, end: Long) extends ReferenceRegion

object TestRegion {
  implicit def makeTestRegion(t: (ContigName, Int, Int)): TestRegion = TestRegion(t._1, t._2, t._3)
}
