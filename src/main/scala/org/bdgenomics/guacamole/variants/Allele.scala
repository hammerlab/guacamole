package org.bdgenomics.guacamole.variants

import org.bdgenomics.guacamole.Bases
import org.bdgenomics.guacamole.Bases.BasesOrdering

case class Allele(refBases: Seq[Byte], altBases: Seq[Byte]) extends Ordered[Allele] {
  lazy val isVariant = BasesOrdering.compare(refBases, altBases) != 0

  override def toString: String = "Allele(%s,%s)".format(Bases.basesToString(refBases), Bases.basesToString(altBases))

  override def compare(that: Allele): Int = AlleleOrdering.compare(this, that)
  def ==(other: Allele): Boolean = compare(other) == 0
}

