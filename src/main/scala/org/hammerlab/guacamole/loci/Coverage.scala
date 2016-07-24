package org.hammerlab.guacamole.loci

case class Coverage(depth: Int = 0, starts: Int = 0) {
  def +(o: Coverage): Coverage =
    Coverage(
      depth + o.depth,
      starts + o.starts
    )

  override def toString: String = s"($depth,$starts)"
}
