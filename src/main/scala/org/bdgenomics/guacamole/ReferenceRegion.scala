package org.bdgenomics.guacamole

/**
 *    TODO: replace with ReferenceRegion base class in ADAM
 */
trait ReferenceRegion {

  /* Name of the reference contig */
  val referenceContig: String

  /* Start position on the genome */
  val start: Long

  /* End position on the genome */
  val end: Long

}
