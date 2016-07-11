package org.hammerlab.guacamole.loci.iterator

import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.reads.ReadsUtil
import org.hammerlab.guacamole.readsets.iterator.overlaps.{LociIntervals, LociOverlapsIterator}
import org.hammerlab.guacamole.reference.Position
import org.scalatest.FunSuite

class IntersectLociIteratorSuite extends FunSuite with ReadsUtil {

  def checkReads(
    halfWindowSize: Int,
    loci: String
  )(
    intervals: (Int, Int, Int)*
  )(
    expected: (Int, String)*
  ): Unit = {

    val contig = "chr1"

    val it =
      new IntersectLociIterator(
        LociSet(loci).onContig("chr1").iterator,
        new LociOverlapsIterator(
          halfWindowSize,
          makeIntervals(intervals)
        )
      )

    checkReads(
      for {
        LociIntervals(locus, reads) <- it
      } yield
        Position(contig, locus) -> reads,
      (for {
        (locus, str) <- expected
      } yield
        contig -> locus -> str
      ).toMap
    )
  }

  test("hello world") {
    checkReads(
      halfWindowSize = 0,
      loci = List(
        "chr1:99-103",
        "chr1:199-202"
      ).mkString(",")
    )(
      (100, 200, 1),
      (101, 201, 1)
    )(
      100 -> "[100,200)",
      101 -> "[100,200), [101,201)",
      102 -> "[100,200), [101,201)",
      199 -> "[100,200), [101,201)",
      200 -> "[101,201)"
    )
  }

  test("simple reads, no window") {
    checkReads(
      halfWindowSize = 0,
      loci = List(
        "chr1:99-103",
        "chr1:198-202",
        "chr1:299-302",
        "chr1:399-401"
      ).mkString(",")
    )(
      (100, 200, 1),
      (101, 201, 1),
      (199, 299, 1),
      (200, 300, 1),
      (300, 400, 1)
    )(
      100 -> "[100,200)",
      101 -> "[100,200), [101,201)",
      102 -> "[100,200), [101,201)",
      198 -> "[100,200), [101,201)",
      199 -> "[100,200), [101,201), [199,299)",
      200 -> "[101,201), [199,299), [200,300)",
      201 -> "[199,299), [200,300)",
      299 -> "[200,300)",
      300 -> "[300,400)",
      301 -> "[300,400)",
      399 -> "[300,400)"
    )
  }

  test("simple reads, window 1") {
    checkReads(
      halfWindowSize = 1,
      loci = List(
        "chr1:98-102",
        "chr1:197-203",
        "chr1:299-302",
        "chr1:400-402"
      ).mkString(",")
    )(
      (100, 200, 1),
      (101, 201, 1),
      (199, 299, 1),
      (200, 300, 1),
      (300, 400, 1)
    )(
       99 -> "[100,200)",
      100 -> "[100,200), [101,201)",
      101 -> "[100,200), [101,201)",
      197 -> "[100,200), [101,201)",
      198 -> "[100,200), [101,201), [199,299)",
      199 -> "[100,200), [101,201), [199,299), [200,300)",
      200 -> "[100,200), [101,201), [199,299), [200,300)",
      201 -> "[101,201), [199,299), [200,300)",
      202 -> "[199,299), [200,300)",
      299 -> "[199,299), [200,300), [300,400)",
      300 -> "[200,300), [300,400)",
      301 -> "[300,400)",
      400 -> "[300,400)"
    )
  }

  test("contained reads") {
    checkReads(
      halfWindowSize = 1,
      loci = List(
        "chr1:99-102",
        "chr1:148-150",
        "chr1:153-155",
        "chr1:160-162",
        "chr1:198-202",
        "chr1:255-257"
      ).mkString(",")
    )(
      (100, 200, 1),
      (101, 199, 1),
      (102, 198, 1),
      (150, 160, 1),
      (155, 255, 1)
    )(
       99 -> "[100,200)",
      100 -> "[100,200), [101,199)",
      101 -> "[100,200), [101,199), [102,198)",
      148 -> "[100,200), [101,199), [102,198)",
      149 -> "[100,200), [101,199), [102,198), [150,160)",
      153 -> "[100,200), [101,199), [102,198), [150,160)",
      154 -> "[100,200), [101,199), [102,198), [150,160), [155,255)",
      160 -> "[100,200), [101,199), [102,198), [150,160), [155,255)",
      161 -> "[100,200), [101,199), [102,198), [155,255)",
      198 -> "[100,200), [101,199), [102,198), [155,255)",
      199 -> "[100,200), [101,199), [155,255)",
      200 -> "[100,200), [155,255)",
      201 -> "[155,255)",
      255 -> "[155,255)"
    )
  }

  test("many reads") {
    checkReads(
      halfWindowSize = 1,
      loci = List(
        "chr1:98-100",
        "chr1:108-111",
        "chr1:119-122",
        "chr1:199-202"
      ).mkString(",")
    )(
      (100, 200, 100000),
      (110, 120, 100000)
    )(
       99 -> "[100,200)*100000",
      108 -> "[100,200)*100000",
      109 -> "[100,200)*100000, [110,120)*100000",
      110 -> "[100,200)*100000, [110,120)*100000",
      119 -> "[100,200)*100000, [110,120)*100000",
      120 -> "[100,200)*100000, [110,120)*100000",
      121 -> "[100,200)*100000",
      199 -> "[100,200)*100000",
      200 -> "[100,200)*100000"
    )
  }

  test("skip gaps and all reads") {
    checkReads(
      halfWindowSize = 1,
      loci = List(
        "chr1:50-52",
        "chr1:60-62",
        "chr1:150-152",
        "chr1:161-163"
      ).mkString(",")
    )(
      (100, 110, 10),
      (120, 130, 10),
      (153, 160, 10)
    )()
  }

  test("skip gaps and some reads") {
    checkReads(
      halfWindowSize = 1,
      loci = List(
        "chr1:50-52",
        "chr1:60-64",
        "chr1:150-153"
      ).mkString(",")
    )(
      (62, 70, 10),
      (80, 90, 100)
    )(
      61 -> "[62,70)*10",
      62 -> "[62,70)*10",
      63 -> "[62,70)*10"
    )
  }

}
