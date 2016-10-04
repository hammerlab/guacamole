package org.hammerlab.guacamole.alignment

import org.hammerlab.guacamole.alignment.AffineGapPenaltyAlignment.{align, scoreAlignmentPaths}
import org.hammerlab.guacamole.util.BasesUtil._
import org.scalatest.{FunSuite, Matchers}

class AffineGapPenaltyAlignmentSuite extends FunSuite with Matchers {

  test("score alignment: exact match") {
    val alignments =
      scoreAlignmentPaths(
        "TCGA",
        "TCGA",
        mismatchProbability = 1e-2,
        openGapProbability = 1e-3,
        closeGapProbability = 1e-2
      )

    alignments(0)._3.toInt should be(0)
  }

  test("score alignment: single mismatch") {
    val alignments =
      scoreAlignmentPaths(
        "TCGA",
        "TCCA",
        mismatchProbability = 1e-2,
        openGapProbability = 1e-3,
        closeGapProbability = 1e-2
      )

    math.round(alignments(0)._3) should be(5)
  }

  test("align exact match") {
    align("TCGA", "TCGA").toCigarString should be("4=")
  }

  test("align: single mismatch") {
    align("TCGA", "TCCA").toCigarString should be("2=1X1=")
  }

  test("align long exact match") {
    val sequence = "TCGATGATCTGAGA"
    align(sequence, sequence).toCigarString should be(sequence.length.toString + "=")
  }

  test("short align with insertion; left aligned") {
    align("TCCGA", "TCGA").toCigarString should be("1=1I3=")
  }

  test("long align with insertion") {
    align("TCGACCCTCTGA", "TCGATCTGA").toCigarString should be("4=3I5=")
  }

  test("long align with deletion") {
    align("TCGATCTGA", "TCGACCCTCTGA").toCigarString should be("4=3D5=")
  }

  test("mixed mismatch and insertion") {
    align("TCGACCCTCTTA", "TCGATCTGA").toCigarString should be("4=3I3=1X1=")
  }

  test("only mismatch long sequence") {
    val alignment =
      align(
      // ====================X===================X============================================================
        "ATTCTCAAGTTTTAAGTGGTATTCTAATTATGGCAGTAATTAACTGAATAAAGAGATTCATCATGTGCAAAAACTAATCTTGTTTACTTAAAATTGAGAGT",
        "ATTCTCAAGTTTTAAGTGGTTTTCTAATTATGGCAGTAATAAACTGAATAAAGAGATTCATCATGTGCAAAAACTAATCTTGTTTACTTAAAATTGAGAGT"
      )

    alignment.toCigarString should be("20=1X19=1X60=")
  }

  test("2 mismatch with deletion sequence") {
    val alignment =
      align(
      // ====================X===================X========================================DDD====================
        "ATTCTCAAGTTTTAAGTGGTATTCTAATTATGGCAGTAATTAACTGAATAAAGAGATTCATCATGTGCAAAAACTAATCTT"+"GTTTACTTAAAATTGAGAGT",
        "ATTCTCAAGTTTTAAGTGGTTTTCTAATTATGGCAGTAATAAACTGAATAAAGAGATTCATCATGTGCAAAAACTAATCTTCCCGTTTACTTAAAATTGAGAGT"
      )

    alignment.toCigarString should be("20=1X19=1X40=3D20=")
  }

  test("left aligning a deletion alignment") {
    val alignment =
      align(
      // ====================================DD   ========================================
        "AGACACGGAGACACACAGAGATACACGGAAACACAG"  +"ACATGCACACACGCGAAGACACAGACACATACACATGCAT",
        "AGACACGGAGACACACAGAGATACACGGAAACACAGAC"+"ACATGCACACACGCGAAGACACAGACACATACACATGCAT"
      )

    alignment.toCigarString should be("36=2D40=")
  }
}
