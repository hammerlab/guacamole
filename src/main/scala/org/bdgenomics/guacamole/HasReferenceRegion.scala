package org.bdgenomics.guacamole

/**
 *    TODO: replace with ReferenceRegion base class in ADAM
 */
trait HasReferenceRegion {

  /* Name of the reference contig */
  val referenceContig: String

  /* Start position on the genome */
  val start: Long

  /* End position on the genome */
  val end: Long

  /**
   * Does this read overlap any of the given loci, with halfWindowSize padding?
   */
  def overlapsLociSet(loci: LociSet, halfWindowSize: Long = 0): Boolean = {
    loci.onContig(referenceContig).intersects(math.max(0, start - halfWindowSize), end + halfWindowSize)
  }

  /**
   * Does the read overlap the given locus, with halfWindowSize padding?
   */
  def overlapsLocus(locus: Long, halfWindowSize: Long = 0): Boolean = {
    start - halfWindowSize <= locus && end + halfWindowSize > locus
  }

}
