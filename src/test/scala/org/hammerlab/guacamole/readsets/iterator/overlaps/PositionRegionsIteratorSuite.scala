//package org.hammerlab.guacamole.readsets.iterator.overlaps
//
//import org.hammerlab.guacamole.loci.set.LociSet
//import org.hammerlab.guacamole.reads.ReadsUtil
//import org.scalatest.FunSuite
//
//class PositionRegionsIteratorSuite extends FunSuite with ReadsUtil {
//
//  def checkReads(
//    halfWindowSize: Int,
//    loci: String,
//    forceLoci: String = ""
//  )(
//    reads: (String, Int, Int, Int)*
//  )(
//    expected: ((String, Int), String)*
//  ): Unit = {
//
//    val it =
//      new OverlappingRegionsIterator(
//        halfWindowSize,
//        LociSet(loci),
//        LociSet(forceLoci),
//        makeReads(reads)
//      )
//
//    checkReads(it, expected.toMap)
//  }
//
//  test("force-call") {
//    checkReads(
//      0,
//      "chr1:100-101",
//      "chr1:100-101"
//    )(
//      ("chr1", 99, 102, 1)
//    )(
//      "chr1" -> 100 -> "[99,102)"
//    )
//  }
//
//  test("hello world") {
//    checkReads(
//      2,
//      "chr1:0-108",
//      "chr1:60-61,chr1:101-103"
//    )(
//      ("chr1", 100, 105, 1),
//      ("chr1", 101, 106, 1)
//    )(
//      "chr1" ->  60 -> "",
//      "chr1" ->  98 -> "[100,105)",
//      "chr1" ->  99 -> "[100,105), [101,106)",
//      "chr1" -> 100 -> "[100,105), [101,106)",
//      "chr1" -> 101 -> "[100,105), [101,106)",
//      "chr1" -> 102 -> "[100,105), [101,106)",
//      "chr1" -> 103 -> "[100,105), [101,106)",
//      "chr1" -> 104 -> "[100,105), [101,106)",
//      "chr1" -> 105 -> "[100,105), [101,106)",
//      "chr1" -> 106 -> "[100,105), [101,106)",
//      "chr1" -> 107 -> "[101,106)"
//    )
//  }
//
//  test("more complex") {
//    checkReads(
//      1,
//      List(
//        "chr1:50-52",
//        "chr1:60-62",
//        "chr1:98-102",
//        "chr1:199-203",
//        "chr2:10-12",
//        "chr2:100-102",
//        "chr4:10-12",
//        "chr5:100-102"
//      ).mkString(","),
//      "chr1:61-63,chr3:10-11"
//    )(
//      ("chr1", 100, 200,  1),
//      ("chr1", 101, 201,  1),
//      ("chr2",   8,   9,  1),
//      ("chr2",  13,  15,  1),
//      ("chr2",  90, 100,  1),
//      ("chr2", 102, 105,  1),
//      ("chr2", 103, 106,  1),
//      ("chr2", 110, 120,  1),
//      ("chr3", 100, 200,  1),
//      ("chr5",  90, 110, 10)
//    )(
//      "chr1" ->  61 -> "",
//      "chr1" ->  62 -> "",
//      "chr1" ->  99 -> "[100,200)",
//      "chr1" -> 100 -> "[100,200), [101,201)",
//      "chr1" -> 101 -> "[100,200), [101,201)",
//      "chr1" -> 199 -> "[100,200), [101,201)",
//      "chr1" -> 200 -> "[100,200), [101,201)",
//      "chr1" -> 201 -> "[101,201)",
//      "chr2" -> 100 -> "[90,100)",
//      "chr2" -> 101 -> "[102,105)",
//      "chr3" ->  10 -> "",
//      "chr5" -> 100 -> "[90,110)*10",
//      "chr5" -> 101 -> "[90,110)*10"
//    )
//  }
//
//  test("boundaries") {
//    checkReads(
//      0,
//      "chr1:100-102",
//      ""
//    )(
//      ("chr1",  91, 101, 1),
//      ("chr1",  92, 102, 1),
//      ("chr1",  95, 105, 1),
//      ("chr1",  99, 109, 2),
//      ("chr1", 100, 110, 1),
//      ("chr1", 101, 111, 1)
//    )(
//      "chr1" -> 100 -> "[91,101), [92,102), [95,105), [99,109)*2, [100,110)",
//      "chr1" -> 101 -> "[92,102), [95,105), [99,109)*2, [100,110), [101,111)"
//    )
//  }
//}
