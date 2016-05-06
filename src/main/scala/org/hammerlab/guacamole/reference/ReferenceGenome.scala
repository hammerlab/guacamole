package org.hammerlab.guacamole.reference

import htsjdk.samtools.Cigar
import org.bdgenomics.adam.util.MdTag
import org.hammerlab.guacamole.util.Bases

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

  /**
   * Build a new MDTag from the reference and read sequence
   *
   * @param readSequence Bases from the read
   * @param referenceContig Name of the reference contig or chromosome
   * @param referenceStart Start position of the read on the reference
   * @param cigar CIGAR for the read on the reference
   * @return MdTag for the the read
   */
  def buildMdTag(readSequence: String, referenceContig: String, referenceStart: Int, cigar: Cigar): String = {
    val referenceSequence =
      Bases.basesToString(
        getReferenceSequence(referenceContig, referenceStart, referenceStart + cigar.getReferenceLength)
      )
    MdTag(readSequence, referenceSequence, cigar, referenceStart).toString
  }
}
