package org.hammerlab.guacamole.alignment

import org.scalatest.{FunSuite, Matchers}


class ReadAlignmentSuite  extends FunSuite with Matchers {

  test ("test cigar string: all match") {
    val alignment = ReadAlignment(
      Seq(
        AlignmentState.Match,
        AlignmentState.Match,
        AlignmentState.Match,
        AlignmentState.Match,
        AlignmentState.Match,
        AlignmentState.Match), 60)

    alignment.toCigar should be("6=")
  }

  test ("test cigar string: mixed match/insertion") {
    val alignment = ReadAlignment(
      Seq(AlignmentState.Match,
        AlignmentState.Match,
        AlignmentState.Match,
        AlignmentState.Insertion,
        AlignmentState.Insertion,
        AlignmentState.Match), 60)

    alignment.toCigar should be("3=2I1=")
  }


  test ("test cigar string: start with single match") {
    val alignment = ReadAlignment(
      Seq(
        AlignmentState.Match,
        AlignmentState.Insertion,
        AlignmentState.Insertion,
        AlignmentState.Insertion,
        AlignmentState.Insertion,
        AlignmentState.Match), 60)

    alignment.toCigar should be("1=4I1=")
  }

  test ("test cigar string: with mismatch") {
    val alignment = ReadAlignment(
      Seq(
        AlignmentState.Match,
        AlignmentState.Mismatch,
        AlignmentState.Mismatch,
        AlignmentState.Match,
        AlignmentState.Match,
        AlignmentState.Match), 60)

    alignment.toCigar should be("1=2X3=")
  }
}
