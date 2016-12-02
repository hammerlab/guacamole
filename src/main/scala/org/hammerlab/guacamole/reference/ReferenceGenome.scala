package org.hammerlab.guacamole.reference

import org.hammerlab.genomics.reference.ContigSequence

trait ReferenceGenome {

  /**
   * Path where this reference was loaded from, or other description of its provenance (optional).
   *
   * For provenance tracking only. Not guaranteed to be a valid path or on a filesystem that is currently accessible.
   *
   */
  val source: Option[String]

  /**
   * Retrieve a full contig/chromosome sequence
   *
   * @param contigName contig/chromosome to retrieve reference sequence from
   * @return Full sequence associated with the contig
   */
  def getContig(contigName: String): ContigSequence

  /**
   * Retrieve a reference base on a given contig at a given locus
   *
   * @param contigName contig/chromosome to retrieve reference sequence from
   * @param locus position in the sequence to retrieve
   * @return Base at the given reference position
   */
  def getReferenceBase(contigName: String, locus: Int): Byte

  /**
   * Get the reference base at the given reference location
   *
   * @param contigName contig/chromosome to retrieve reference sequence from
   * @param startLocus 0-based inclusive start of the subsequence
   * @param endLocus 0-based exclusive end of the subsequence
   * @return Array of bases for the reference sequence
   */
  def getReferenceSequence(contigName: String, startLocus: Int, endLocus: Int): Array[Byte]
}
