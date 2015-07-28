package org.hammerlab.guacamole.alignment

import org.scalatest.{ FunSuite, Matchers }
import org.hammerlab.guacamole.util.TestUtil.Implicits._

class AffineGapPenaltyAlignmentSuite extends FunSuite with Matchers {

  test("score alignment: exact match") {
    val (alignmentStates, alignmentScores) = AffineGapPenaltyAlignment.scoreAlignmentPaths("TCGA", "TCGA")
    alignmentScores(4, 4).toInt should be(0)
  }

  test("score alignment: single mismatch") {
    val (alignmentStates, alignmentScores) = AffineGapPenaltyAlignment.scoreAlignmentPaths("TCGA", "TCCA", mismatchProbability = 1e-2)
    alignmentScores(4, 4).toInt should be(5)
  }

  test("align exact match") {
    val alignment = AffineGapPenaltyAlignment.align("TCGA", "TCGA")
    alignment.toCigar should be("4=")
  }

  test("align: single mismatch") {
    val alignment = AffineGapPenaltyAlignment.align("TCGA", "TCCA", mismatchProbability = 1e-2)

    alignment.toCigar should be("2=1X1=")
  }

  test("align long exact match") {
    val sequence = "TCGATGATCTGAGA"
    val alignment = AffineGapPenaltyAlignment.align(sequence, sequence)
    alignment.toCigar should be(sequence.length.toString + "=")
  }

  test("short align with insertion") {
    val alignment = AffineGapPenaltyAlignment.align("TCCGA", "TCGA")
    alignment.toCigar should be("2=1I2=")
  }

  test("long align with insertion") {
    val alignment = AffineGapPenaltyAlignment.align("TCGACCCTCTGA", "TCGATCTGA")
    alignment.toCigar should be("4=3I5=")
  }

  test("long align with deletion") {
    val alignment = AffineGapPenaltyAlignment.align("TCGATCTGA", "TCGACCCTCTGA")
    alignment.toCigar should be("4=3D5=")
  }

  test("mixed mismatch and insertion") {
    val alignment = AffineGapPenaltyAlignment.align("TCGACCCTCTTA", "TCGATCTGA")
    alignment.toCigar should be("4=3I3=1X1=")
  }
}
