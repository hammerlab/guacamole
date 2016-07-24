package org.hammerlab.guacamole.reference

import com.esotericsoftware.kryo.Kryo

import scala.math.PartiallyOrdered

// Base trait for classes that logically exist at one genomic locus.
trait Position
  extends ReferenceRegion
    with HasLocus
    with PartiallyOrdered[Position] {

  def contigName: ContigName
  def locus: Locus

  def start = locus
  def end = locus + 1

  override def tryCompareTo[B >: Position](that: B)(implicit ev: (B) => PartiallyOrdered[B]): Option[Int] = {
    that match {
      case other: Position =>
        if (contigName == other.contigName)
          Some(locus.compare(other.locus))
        else
          None
      case _ => None
    }
  }

  override def toString: String = s"$contigName:$locus"
}

private case class PositionImpl(contigName: ContigName, locus: Locus)
  extends Position

object Position {

  def registerKryo(kryo: Kryo): Unit = {
    kryo.register(classOf[Position])
    kryo.register(classOf[Array[Position]])
    kryo.register(classOf[PositionImpl])
    kryo.register(classOf[Array[PositionImpl]])
  }

  def apply(contigName: ContigName, locus: Locus): Position = PositionImpl(contigName, locus)

  def unapply(p: Position): Option[(ContigName, Locus)] = Some((p.contigName, p.locus))

  implicit val ordering = new Ordering[Position] {
    override def compare(x: Position, y: Position): Int = {
      val contigCmp = x.contigName.compare(y.contigName)
      if (contigCmp == 0)
        x.locus.compare(y.locus)
      else
        contigCmp
    }
  }
}
