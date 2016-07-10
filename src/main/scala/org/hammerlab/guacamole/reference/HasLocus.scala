package org.hammerlab.guacamole.reference

import org.hammerlab.guacamole.reference.Position.Locus

trait HasLocus {
  def locus: Locus
}

object HasLocus {
  implicit def hasLocusToLocus(hl: HasLocus): Locus = hl.locus
}
