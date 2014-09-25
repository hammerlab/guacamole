package org.bdgenomics.guacamole.variants

import com.esotericsoftware.kryo.io.{ Input, Output }
import com.esotericsoftware.kryo.{ Kryo, Serializer }
import org.bdgenomics.guacamole.Bases
import org.bdgenomics.guacamole.Bases.BasesOrdering

case class Allele(refBases: Seq[Byte], altBases: Seq[Byte]) {
  lazy val isVariant = BasesOrdering.compare(refBases, altBases) != 0

  override def toString: String = "Allele(%s,%s)".format(Bases.basesToString(refBases), Bases.basesToString(altBases))

  def ==(that: Allele): Boolean = Allele.ordering.compare(this, that) == 0
}

object Allele {
  implicit val ordering: Ordering[Allele] = new Ordering[Allele] {
    override def compare(x: Allele, y: Allele): Int = {
      BasesOrdering.compare(x.refBases, y.refBases) match {
        case 0 => BasesOrdering.compare(x.altBases, y.altBases)
        case x => x
      }
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
