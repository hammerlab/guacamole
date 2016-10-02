package org.hammerlab.guacamole.alignment

import org.hammerlab.guacamole.alignment.ReadAlignment.scoreAlignmentPaths
import org.hammerlab.guacamole.util.BasesUtil._
import org.scalatest.{FunSuite, Matchers}

class ReadAlignmentSuite extends FunSuite with Matchers {

  test("test cigar string: all match") {
    val alignment = ReadAlignment(
      Seq(
        AlignmentState.Match,
        AlignmentState.Match,
        AlignmentState.Match,
        AlignmentState.Match,
        AlignmentState.Match,
        AlignmentState.Match
      ),
      0, 5,
      60
    )

    alignment.toCigarString should be("6=")
  }

  test("test cigar string: mixed match/insertion") {
    val alignment = ReadAlignment(
      Seq(
        AlignmentState.Match,
        AlignmentState.Match,
        AlignmentState.Match,
        AlignmentState.Insertion,
        AlignmentState.Insertion,
        AlignmentState.Match
      ),
      0, 6,
      60
    )

    alignment.toCigarString should be("3=2I1=")
  }

  test("test cigar string: start with single match") {
    val alignment = ReadAlignment(
      Seq(
        AlignmentState.Match,
        AlignmentState.Insertion,
        AlignmentState.Insertion,
        AlignmentState.Insertion,
        AlignmentState.Insertion,
        AlignmentState.Match
      ),
      0, 5,
      60
    )

    alignment.toCigarString should be("1=4I1=")
  }

  test("test cigar string: with mismatch") {
    val alignment = ReadAlignment(
      Seq(
        AlignmentState.Match,
        AlignmentState.Mismatch,
        AlignmentState.Mismatch,
        AlignmentState.Match,
        AlignmentState.Match,
        AlignmentState.Match
      ),
      0, 5,
      60
    )

    alignment.toCigarString should be("1=2X3=")
  }

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
    ReadAlignment("TCGA", "TCGA").toCigarString should be("4=")
  }

  test("align: single mismatch") {
    ReadAlignment("TCGA", "TCCA").toCigarString should be("2=1X1=")
  }

  test("align long exact match") {
    val sequence = "TCGATGATCTGAGA"
    ReadAlignment(sequence, sequence).toCigarString should be(sequence.length.toString + "=")
  }

  test("short align with insertion; left aligned") {
    ReadAlignment("TCCGA", "TCGA").toCigarString should be("1=1I3=")
  }

  test("long align with insertion") {
    ReadAlignment("TCGACCCTCTGA", "TCGATCTGA").toCigarString should be("4=3I5=")
  }

  test("long align with deletion") {
    ReadAlignment("TCGATCTGA", "TCGACCCTCTGA").toCigarString should be("4=3D5=")
  }

  test("mixed mismatch and insertion") {
    ReadAlignment("TCGACCCTCTTA", "TCGATCTGA").toCigarString should be("4=3I3=1X1=")
  }

  test("only mismatch long sequence") {
    val alignment = ReadAlignment(
      // ====================X===================X============================================================
        "ATTCTCAAGTTTTAAGTGGTATTCTAATTATGGCAGTAATTAACTGAATAAAGAGATTCATCATGTGCAAAAACTAATCTTGTTTACTTAAAATTGAGAGT",
        "ATTCTCAAGTTTTAAGTGGTTTTCTAATTATGGCAGTAATAAACTGAATAAAGAGATTCATCATGTGCAAAAACTAATCTTGTTTACTTAAAATTGAGAGT")

    alignment.toCigarString should be("20=1X19=1X60=")
  }

  test("2 mismatch with deletion sequence") {
    val alignment = ReadAlignment(
      // ====================X===================X========================================DDD====================
        "ATTCTCAAGTTTTAAGTGGTATTCTAATTATGGCAGTAATTAACTGAATAAAGAGATTCATCATGTGCAAAAACTAATCTT"+"GTTTACTTAAAATTGAGAGT",
        "ATTCTCAAGTTTTAAGTGGTTTTCTAATTATGGCAGTAATAAACTGAATAAAGAGATTCATCATGTGCAAAAACTAATCTTCCCGTTTACTTAAAATTGAGAGT")

    alignment.toCigarString should be("20=1X19=1X40=3D20=")
  }

  test("left aligning a deletion alignment") {
    val alignment = ReadAlignment(
      // ====================================DD   ========================================
        "AGACACGGAGACACACAGAGATACACGGAAACACAG"  +"ACATGCACACACGCGAAGACACAGACACATACACATGCAT",
        "AGACACGGAGACACACAGAGATACACGGAAACACAGAC"+"ACATGCACACACGCGAAGACACAGACACATACACATGCAT")

    alignment.toCigarString should be("36=2D40=")
  }
}
