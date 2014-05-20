package org.bdgenomics.guacamole.pileup

/**
 * The Alignment of a read at a particular locus specifies:
 *
 *  - the Cigar operator for this read and locus.
 *
 *  - the base(s) read at the corresponding offset in the read.
 *
 *  - the base quality scores of the bases read.
 */
private[pileup] sealed abstract class Alignment
case class Insertion(bases: String, baseQualities: Array[Int]) extends Alignment
case class Deletion() extends Alignment
case class Match(base: Char, baseQuality: Int) extends Alignment
case class Mismatch(base: Char, baseQuality: Int) extends Alignment