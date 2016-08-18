package org.hammerlab.guacamole.variants

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import org.hammerlab.guacamole.util.Bases
import org.hammerlab.guacamole.util.Bases.BasesOrdering

case class Allele(refBases: Seq[Byte], altBases: Seq[Byte]) extends Ordered[Allele] {
  val isVariant = refBases != altBases

  override def toString: String = "Allele(%s,%s)".format(Bases.basesToString(refBases), Bases.basesToString(altBases))

  override def compare(that: Allele): Int = {
    BasesOrdering.compare(refBases, that.refBases) match {
      case 0 => BasesOrdering.compare(altBases, that.altBases)
      case x => x
    }
  }
}

object Allele {
  def apply(refBases: String, altBases: String): Allele = {
    Allele(Bases.stringToBases(refBases), Bases.stringToBases(altBases))
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
    val referenceBases: IndexedSeq[Byte] = input.readBytes(referenceBasesLength)
    val alternateLength = input.readInt(true)
    val alternateBases: IndexedSeq[Byte] = input.readBytes(alternateLength)
    Allele(referenceBases, alternateBases)
  }
}

trait HasAlleleSerializer {
  lazy val alleleSerializer: AlleleSerializer = new AlleleSerializer
}
