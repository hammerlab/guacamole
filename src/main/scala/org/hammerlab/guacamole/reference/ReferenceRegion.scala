package org.hammerlab.guacamole.reference

/**
 * Trait for objects that are associated with an interval on the genome. The most prominent example is a
 * [[org.hammerlab.guacamole.reads.MappedRead]], but there's also [[org.hammerlab.guacamole.variants.ReferenceVariant]].
 */
trait ReferenceRegion
  extends HasContig
    with Interval {

  /** Name of the reference contig */
  def contigName: ContigName

  /**
   * Does the region overlap the given locus, with halfWindowSize padding?
   */
  def overlapsLocus(locus: Locus, halfWindowSize: Int = 0): Boolean = {
    start - halfWindowSize <= locus && end + halfWindowSize > locus
  }

  /**
   * Does the region overlap another reference region
   *
   * @param other another region on the genome
   * @return True if the the regions overlap
   */
  def overlaps(other: ReferenceRegion): Boolean = {
    other.contigName == contigName && (overlapsLocus(other.start) || other.overlapsLocus(start))
  }

  def regionStr: String = s"$contigName:[$start-$end)"
}

object ReferenceRegion {
  implicit def intraContigPartialOrdering[R <: ReferenceRegion] =
    new PartialOrdering[R] {
      override def tryCompare(x: R, y: R): Option[Int] = {
        if (x.contigName == y.contigName)
          Some(x.start.compare(y.start))
        else
          None
      }

      override def lteq(x: R, y: R): Boolean = {
        x.contigName == y.contigName && x.start <= y.start
      }
    }

  def unapply(region: ReferenceRegion): Option[(ContigName, Locus, Locus)] =
    Some(
      region.contigName,
      region.start,
      region.end
    )
}
