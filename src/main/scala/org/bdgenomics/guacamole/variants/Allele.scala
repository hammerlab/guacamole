package org.bdgenomics.guacamole.variants

import com.esotericsoftware.kryo.io.{ Input, Output }
import com.esotericsoftware.kryo.{ Kryo, Serializer }
import org.bdgenomics.guacamole.Bases
import org.bdgenomics.guacamole.Bases.BasesOrdering

case class Allele(refBases: Seq[Byte], altBases: Seq[Byte]) extends Ordered[Allele] {
  lazy val isVariant = refBases != altBases

  override def toString: String = "Allele(%s,%s)".format(Bases.basesToString(refBases), Bases.basesToString(altBases))

  override def equals(other: Any): Boolean = other match {
    case otherAllele: Allele => refBases == otherAllele.refBases && altBases == otherAllele.altBases
    case _                   => false
  }
  def ==(other: Allele): Boolean = equals(other)

  override def compare(that: Allele): Int = {
    BasesOrdering.compare(refBases, that.refBases) match {
      case 0 => BasesOrdering.compare(altBases, that.altBases)
      case x => x
    }
  }
}

class AlleleSerializer extends Serializer[Allele] {
  def write(kryo: Kryo, output: Output, obj: Allele) = {
    output.writeInt(obj.refBases.length, true)
    output.writeBytes(obj.refBases.toArray)
    output.writeInt(obj.altBases.length, true)
    output.writeBytes(obj.altBases.toArray)
  }

  def read(kryo: Kryo, input: Input, klass: Class[Allele]): Allele = {
    val referenceBasesLength = input.readInt(true)
    val referenceBases: Seq[Byte] = input.readBytes(referenceBasesLength)
    val alternateLength = input.readInt(true)
    val alternateBases: Seq[Byte] = input.readBytes(alternateLength)
    Allele(referenceBases, alternateBases)
  }
}

trait HasAlleleSerializer {
  lazy val alleleSerializer: AlleleSerializer = new AlleleSerializer
}
