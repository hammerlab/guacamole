package org.hammerlab.guacamole.alignment

import org.hammerlab.guacamole.alignment.AlignmentState.{Insertion, Match, Mismatch}
import org.scalatest.{FunSuite, Matchers}

class ReadAlignmentSuite extends FunSuite with Matchers {

  test("test cigar string: all match") {
    val alignment = ReadAlignment(
      Seq(
        Match,
        Match,
        Match,
        Match,
        Match,
        Match
      ),
      0, 5,
      60
    )

    alignment.toCigarString should be("6=")
  }

  test("test cigar string: mixed match/insertion") {
    val alignment = ReadAlignment(
      Seq(
        Match,
        Match,
        Match,
        Insertion,
        Insertion,
        Match
      ),
      0, 6,
      60
    )

    alignment.toCigarString should be("3=2I1=")
  }

  test("test cigar string: start with single match") {
    val alignment = ReadAlignment(
      Seq(
        Match,
        Insertion,
        Insertion,
        Insertion,
        Insertion,
        Match
      ),
      0, 5,
      60
    )

    alignment.toCigarString should be("1=4I1=")
  }

  test("test cigar string: with mismatch") {
    val alignment = ReadAlignment(
      Seq(
        Match,
        Mismatch,
        Mismatch,
        Match,
        Match,
        Match
      ),
      0, 5,
      60
    )

    alignment.toCigarString should be("1=2X3=")
  }
}
