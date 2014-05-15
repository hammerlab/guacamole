package org.bdgenomics.guacamole.pileup

/**
 * The alignment of a read combines the underlying Cigar operator
 * (match/mismatch/deletion/insertion) with the characters which were used from the read.
 */
private[pileup] sealed abstract class Alignment
case class Insertion(bases: String, baseQualities: Array[Int]) extends Alignment
case class Deletion() extends Alignment
case class Match(base: Char, baseQuality: Int) extends Alignment
case class Mismatch(base: Char, baseQuality: Int) extends Alignment