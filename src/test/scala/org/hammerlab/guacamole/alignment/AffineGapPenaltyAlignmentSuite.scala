package org.hammerlab.guacamole.alignment

import org.scalatest.{ FunSuite, Matchers }
import org.hammerlab.guacamole.util.TestUtil.Implicits._

class AffineGapPenaltyAlignmentSuite extends FunSuite with Matchers {

  test("score alignment: exact match") {
    val alignments = AffineGapPenaltyAlignment.scoreAlignmentPaths(
      "TCGA",
      "TCGA",
      mismatchProbability = 1e-2,
      openGapProbability = 1e-3,
      closeGapProbability = 1e-2
    )
    alignments(4, 4)._3.toInt should be(0)
  }

  test("score alignment: single mismatch") {
    val alignments = AffineGapPenaltyAlignment.scoreAlignmentPaths(
      "TCGA",
      "TCCA",
      mismatchProbability = 1e-2,
      openGapProbability = 1e-3,
      closeGapProbability = 1e-2
    )
    math.round(alignments(4, 4)._3) should be(5)
  }

  test("align exact match") {
    val alignment = AffineGapPenaltyAlignment.align("TCGA", "TCGA")
    alignment.toCigar should be("4=")
  }

  test("align: single mismatch") {
    val alignment = AffineGapPenaltyAlignment.align("TCGA", "TCCA")

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

  test("only mismatch long sequence") {
    val alignment = AffineGapPenaltyAlignment
      .align(
        //====================X===================X============================================================
        "ATTCTCAAGTTTTAAGTGGTATTCTAATTATGGCAGTAATTAACTGAATAAAGAGATTCATCATGTGCAAAAACTAATCTTGTTTACTTAAAATTGAGAGT",
        "ATTCTCAAGTTTTAAGTGGTTTTCTAATTATGGCAGTAATAAACTGAATAAAGAGATTCATCATGTGCAAAAACTAATCTTGTTTACTTAAAATTGAGAGT")
    alignment.toCigar should be("20=1X19=1X60=")
  }

  test("2 mismatch with deletion sequence") {
    val alignment = AffineGapPenaltyAlignment
      .align(
        //====================X===================X========================================DDD====================
        "ATTCTCAAGTTTTAAGTGGTATTCTAATTATGGCAGTAATTAACTGAATAAAGAGATTCATCATGTGCAAAAACTAATCTT" + "GTTTACTTAAAATTGAGAGT",
        "ATTCTCAAGTTTTAAGTGGTTTTCTAATTATGGCAGTAATAAACTGAATAAAGAGATTCATCATGTGCAAAAACTAATCTTCCCGTTTACTTAAAATTGAGAGT")
    alignment.toCigar should be("20=1X19=1X40=3D20=")
  }
}
