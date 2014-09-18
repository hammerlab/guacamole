package org.bdgenomics.guacamole.variants

import org.bdgenomics.guacamole.Bases
import org.bdgenomics.guacamole.Bases.BasesOrdering

case class Allele(refBases: Seq[Byte], altBases: Seq[Byte]) {
  lazy val isVariant = BasesOrdering.compare(refBases, altBases) != 0

  override def toString: String = "Allele(%s,%s)".format(Bases.basesToString(refBases), Bases.basesToString(altBases))

  def equals(other: Allele): Boolean = AlleleOrdering.compare(this, other) == 0
  def ==(other: Allele): Boolean = equals(other)
}

