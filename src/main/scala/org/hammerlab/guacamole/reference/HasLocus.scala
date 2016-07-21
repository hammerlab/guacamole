package org.hammerlab.guacamole.reference

trait HasLocus {
  def locus: Locus
}

object HasLocus {
  implicit def hasLocusToLocus(hl: HasLocus): Locus = hl.locus
}
