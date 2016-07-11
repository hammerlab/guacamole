package org.hammerlab.guacamole.reference
import org.hammerlab.guacamole.reference.Position.Locus

import scala.math.PartiallyOrdered

// Base trait for classes that logically exist at one genomic locus.
trait PositionI extends ReferenceRegion with PartiallyOrdered[PositionI] {
  def contig: Contig
  def locus: Locus

  def start = locus
  def end = locus + 1

  override def tryCompareTo[B >: PositionI](that: B)(implicit ev: (B) => PartiallyOrdered[B]): Option[Int] = {
    that match {
      case other: PositionI =>
        if (contig == other.contig)
          Some(locus.compare(other.locus))
        else
          None
      case _ => None
    }
  }
}

case class Position(contig: Contig, locus: Locus)
  extends ReferenceRegion
    with HasLocus
    with PositionI {

  def +(length: Locus): Position = Position(contig, locus + length)
  def -(length: Locus): Position = Position(contig, math.max(0L, locus - length))

  override def toString: String = s"$contig:$locus"
}

object Position {
  implicit val ordering = new Ordering[Position] {
    override def compare(x: Position, y: Position): Int = {
      val contigCmp = x.contig.compare(y.contig)
      if (contigCmp == 0)
        x.locus.compare(y.locus)
      else
        contigCmp
    }
  }

  type Locus = Long
  type NumLoci = Long
}
