package org.hammerlab.guacamole.variants

import org.hammerlab.guacamole.util.Bases.{ BasesOrdering, basesToString, stringToBases }

case class Allele(refBases: Seq[Byte], altBases: Seq[Byte]) extends Ordered[Allele] {
  val isVariant = refBases != altBases

  override def toString: String = "Allele(%s,%s)".format(basesToString(refBases), basesToString(altBases))

  override def compare(that: Allele): Int =
    BasesOrdering.compare(refBases, that.refBases) match {
      case 0 => BasesOrdering.compare(altBases, that.altBases)
      case x => x
    }
}

object Allele {
  def apply(refBases: String, altBases: String): Allele =
    Allele(stringToBases(refBases), stringToBases(altBases))

  def apply(refBase: Byte, altBase: Byte): Allele =
    Allele(Array(refBase), Array(altBase))
}
