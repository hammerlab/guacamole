package org.hammerlab.guacamole.variants

import org.hammerlab.genomics.bases.{ Base, Bases }

case class Allele(refBases: Bases,
                  altBases: Bases) {

  val isVariant = refBases != altBases

  override def toString: String = s"Allele($refBases,$altBases)"
}

object Allele {
  def apply(refBase: Base, altBase: Base): Allele =
    Allele(Bases(refBase), Bases(altBase))

  implicit val ordering = Ordering.by(unapply)
}
