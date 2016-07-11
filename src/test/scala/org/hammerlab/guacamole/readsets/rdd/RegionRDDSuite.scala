package org.hammerlab.guacamole.readsets.rdd

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.loci.Coverage
import org.hammerlab.guacamole.loci.Coverage.PositionCoverage
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.readsets.rdd.RegionRDD._
import org.hammerlab.guacamole.readsets.{ContigLengths, ContigLengthsUtil}
import org.hammerlab.guacamole.reference.{Contig, Position, ReferenceRegion}
import org.hammerlab.guacamole.util.{GuacFunSuite, KryoTestRegistrar}
import org.hammerlab.magic.rdd.CmpStats
import org.hammerlab.magic.rdd.EqualsRDD._

import scala.collection.SortedMap

class RegionRDDSuiteRegistrar extends KryoTestRegistrar {
  override def registerTestClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[Array[TestRegion]])
    kryo.register(classOf[TestRegion])
    kryo.register(classOf[CmpStats])
    kryo.register(Class.forName("scala.Tuple2$mcIZ$sp"))
    kryo.register(classOf[scala.collection.mutable.Queue[_]])
    kryo.register(classOf[scala.collection.mutable.LinkedList[_]])
    kryo.register(classOf[Array[Int]])
  }
}

class RegionRDDSuite extends GuacFunSuite with ReadsRDDUtil with ContigLengthsUtil {

  override def registrar = "org.hammerlab.guacamole.readsets.rdd.RegionRDDSuiteRegistrar"

  def testLoci[T, U](actual: Array[(Position, T)],
                     actualStrs: Array[(String, U)],
                     expected: List[(String, U)]): Unit = {

    val actualMap = SortedMap(actualStrs: _*)
    val expectedMap = SortedMap(expected: _*)

    val extraElems = actualMap.filterKeys(!expectedMap.contains(_))
    val missingElems = expectedMap.filterKeys(!actualMap.contains(_))

    val diffElems =
      for {
        (k, ev) <- expectedMap
        av <- actualMap.get(k)
        if ev != av
      } yield
        k -> (av, ev)

    withClue("differing loci:") { diffElems should be(Map()) }
    withClue("found extra loci:") { extraElems should be(Map()) }
    withClue("missing loci:") { missingElems should be(Map()) }

    withClue("loci out of order:") {
      actual.map(_._1).sortBy(x => x) should be(actual.map(_._1))
    }
  }

  def checkCoverage(rdd: RDD[PositionCoverage], expected: List[(String, (Int, Int, Int))]): Unit = {
    val actual = rdd.collect()
    val actualStrs =
      for {
        (pos, Coverage(depth, starts, ends)) <- actual
      } yield {
        pos.toString -> (depth, starts, ends)
      }

    testLoci(actual, actualStrs, expected)
  }

  def testRDDReads[R <: ReferenceRegion](rdd: RDD[(Position, Iterable[R])],
                                         expected: List[(String, Iterable[(Contig, Int, Int)])]) = {
    val actual = rdd.map(t => t._1 -> t._2.toList).collect()
    val actualStrs: Array[(String, Iterable[(Contig, Int, Int)])] =
      for {
        (pos, reads) <- actual
      } yield
        pos.toString ->
          reads.map(r => (r.contig, r.start.toInt, r.end.toInt))

    testLoci(actual, actualStrs, expected)
  }

  test("coverage") {
    val readsRDD =
      makeReadsRDD(
        1,
        ("chr1", 100, 105,  1),
        ("chr1", 101, 106,  1),
        ("chr2",   8,   9,  1),
        ("chr2",   9,  11,  1),
        ("chr2", 102, 105,  1),
        ("chr2", 103, 106, 10),
        ("chr5",  90,  91, 10)
      )

    val contigLengths: ContigLengths = makeContigLengths("chr1" -> 1000, "chr2" -> 1000, "chr5" -> 1000)
    implicit val contigLengthsBroadcast: Broadcast[ContigLengths] = sc.broadcast(contigLengths)

    val rdd = readsRDD.coverage(0, LociSet.all(contigLengths))

    val expected =
      List(
        "chr1:100" -> ( 1,  1,  0),
        "chr1:101" -> ( 2,  1,  0),
        "chr1:102" -> ( 2,  0,  0),
        "chr1:103" -> ( 2,  0,  0),
        "chr1:104" -> ( 2,  0,  0),
        "chr1:105" -> ( 1,  0,  1),
        "chr1:106" -> ( 0,  0,  1),
          "chr2:8" -> ( 1,  1,  0),
          "chr2:9" -> ( 1,  1,  1),
         "chr2:10" -> ( 1,  0,  0),
         "chr2:11" -> ( 0,  0,  1),
        "chr2:102" -> ( 1,  1,  0),
        "chr2:103" -> (11, 10,  0),
        "chr2:104" -> (11,  0,  0),
        "chr2:105" -> (10,  0,  1),
        "chr2:106" -> ( 0,  0, 10),
         "chr5:90" -> (10, 10,  0),
         "chr5:91" -> ( 0,  0, 10)
      )

    val shuffled = readsRDD.shuffleCoverage(0, contigLengthsBroadcast)

    checkCoverage(rdd, expected)
    checkCoverage(shuffled, expected)

    rdd.compare(shuffled).stats should be(CmpStats(18))
  }

//  test("slidingWindowLoci") {
//    val readsRDD =
//      makeReadsRDD(
//        2,
//        ("chr1", 100, 105,  1),
//        ("chr1", 101, 106,  1),
//        ("chr2",   8,   9,  1),
//        ("chr2",   9,  11,  1),
//        ("chr2", 102, 105,  1),
//        ("chr2", 103, 106, 10),
//        ("chr5",  90,  91, 10)
//      )
//
//    val contigLengths: ContigLengths =
//      Map(
//        "chr1" -> 200,
//        "chr2" -> 200,
//        "chr5" -> 200
//      )
//
//    implicit val contigLengthsBroadcast: Broadcast[ContigLengths] = sc.broadcast(contigLengths)
//
//    val pileups = readsRDD.slidingLociWindow(2, LociSet.all(contigLengths)).mapValues(_.toList).collect().iterator
//
//   val expected =
//     Map(
//       ("chr1",  98) -> "[100,105)",
//       ("chr1",  99) -> "[100,105), [101,106)",
//       ("chr1", 100) -> "[100,105), [101,106)",
//       ("chr1", 101) -> "[100,105), [101,106)",
//       ("chr1", 102) -> "[100,105), [101,106)",
//       ("chr1", 103) -> "[100,105), [101,106)",
//       ("chr1", 104) -> "[100,105), [101,106)",
//       ("chr1", 105) -> "[100,105), [101,106)",
//       ("chr1", 106) -> "[100,105), [101,106)",
//       ("chr1", 107) -> "[101,106)",
//       ("chr2",   6) -> "[8,9)",
//       ("chr2",   7) -> "[8,9), [9,11)",
//       ("chr2",   8) -> "[8,9), [9,11)",
//       ("chr2",   9) -> "[8,9), [9,11)",
//       ("chr2",  10) -> "[8,9), [9,11)",
//       ("chr2",  11) -> "[9,11)",
//       ("chr2",  12) -> "[9,11)",
//       ("chr2", 100) -> "[102,105)",
//       ("chr2", 101) -> "[102,105), [103,106)*10",
//       ("chr2", 102) -> "[102,105), [103,106)*10",
//       ("chr2", 103) -> "[102,105), [103,106)*10",
//       ("chr2", 104) -> "[102,105), [103,106)*10",
//       ("chr2", 105) -> "[102,105), [103,106)*10",
//       ("chr2", 106) -> "[102,105), [103,106)*10",
//       ("chr2", 107) -> "[103,106)*10",
//       ("chr5",  88) -> "[90,91)*10",
//       ("chr5",  89) -> "[90,91)*10",
//       ("chr5",  90) -> "[90,91)*10",
//       ("chr5",  91) -> "[90,91)*10",
//       ("chr5",  92) -> "[90,91)*10"
//     )
//
//    checkReads(pileups, expected)
//  }
}
