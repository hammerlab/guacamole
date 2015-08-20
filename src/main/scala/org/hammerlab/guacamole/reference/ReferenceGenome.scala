package org.hammerlab.guacamole.reference

import htsjdk.samtools.Cigar
import org.bdgenomics.adam.util.MdTag
import org.hammerlab.guacamole.Bases

trait ReferenceGenome {

  /**
   * Retrieve a full contig sequence
   * @param contigName contig/chromosome to retrieve reference sequence from
   * @return Full sequence associated with the contig
   */
  def getContig(contigName: String): Array[Byte]

  /**
   * Retrieve a reference base on a given contig at a given locus
   * @param contigName contig/chromosome to retrieve reference sequence from
   * @param locus position in the sequence to retrieve
   * @return Base at the given reference position
   */
  def getReferenceBase(contigName: String, locus: Int): Byte

  /**
   * *
   * @param contigName contig/chromosome to retrieve reference sequence from
   * @param startLocus 0-based inclusive start of the subsequence
   * @param endLocus 0-based exclusive end of the subsequence
   * @return
   */
  def getReferenceSequence(contigName: String, startLocus: Int, endLocus: Int): Array[Byte]

  def buildMdTag(readSequence: String, referenceContig: String, referenceStart: Int, cigar: Cigar): String = {
    val referenceSequence =
      Bases.basesToString(
        getReferenceSequence(referenceContig, referenceStart, referenceStart + cigar.getReferenceLength)
      )
    MdTag(readSequence, referenceSequence, cigar, referenceStart).toString
  }

}
