package org.hammerlab.guacamole.reference

trait Interval {
  def start: Locus
  def end: Locus

  def contains(locus: Locus): Boolean = start <= locus && locus < end
}

object Interval {
  def orderByEndDesc[I <: Interval] = new Ordering[I] {
    override def compare(x: I, y: I): Int = y.end.compare(x.end)
  }
}
