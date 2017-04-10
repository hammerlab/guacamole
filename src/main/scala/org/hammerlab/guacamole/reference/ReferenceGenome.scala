package org.hammerlab.guacamole.reference

import org.hammerlab.genomics.bases.{ Base, Bases }
import org.hammerlab.genomics.reference.{ ContigName, Contig, Locus }
import org.hammerlab.paths.Path

trait ReferenceGenome {

  /**
   * Path where this reference was loaded from, or other description of its provenance (optional).
   *
   * For provenance tracking only. Not guaranteed to be a valid path or on a filesystem that is currently accessible.
   */
  def source: Option[Path]

  /**
   * Retrieve a full contig/chromosome sequence
   *
   * @param contigName contig/chromosome to retrieve reference sequence from
   * @return Full sequence associated with the contig
   */
  def apply(contigName: ContigName): Contig

  /**
   * Retrieve a reference base on a given contig at a given locus
   *
   * @param contigName contig/chromosome to retrieve reference sequence from
   * @param locus position in the sequence to retrieve
   * @return Base at the given reference position
   */
  def apply(contigName: ContigName, locus: Locus): Base = apply(contigName)(locus)

  /**
   * Get the reference base at the given reference location
   *
   * @param contigName contig/chromosome to retrieve reference sequence from
   * @param start 0-based inclusive start of the subsequence
   * @param length number of bases to take, starting from startLocus. Integer; strictly intended for
   *               k-mer/assembly-window-type calculations.
   * @return Array of bases for the reference sequence
   */
  def apply(contigName: ContigName, start: Locus, length: Int): Bases = apply(contigName).slice(start, length)
}
