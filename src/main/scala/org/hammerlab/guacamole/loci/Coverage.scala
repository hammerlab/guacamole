package org.hammerlab.guacamole.loci

import org.hammerlab.guacamole.reference.Position

case class Coverage(depth: Int = 0, starts: Int = 0, ends: Int = 0) {
  def +(o: Coverage): Coverage =
    Coverage(
      depth + o.depth,
      starts + o.starts,
      ends + o.ends
    )

  override def toString: String = s"($depth,$starts,$ends)"
}

object Coverage {
  type PositionCoverage = (Position, Coverage)
}
