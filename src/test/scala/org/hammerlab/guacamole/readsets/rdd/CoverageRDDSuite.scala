package org.hammerlab.guacamole.readsets.rdd

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.rdd.RDD
import org.hammerlab.guacamole.loci.Coverage
import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.reads.TestRegion
import org.hammerlab.guacamole.readsets.{ContigLengths, ContigLengthsUtil}
import org.hammerlab.guacamole.reference.Position
import org.hammerlab.guacamole.util.{GuacFunSuite, KryoTestRegistrar}
import org.hammerlab.magic.rdd.cmp.CmpStats
import org.hammerlab.magic.rdd.cmp.EqualsRDD._
import org.hammerlab.magic.test.SeqMatcher.seqMatch

class CoverageRDDSuiteRegistrar extends KryoTestRegistrar {
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

class CoverageRDDSuite
  extends GuacFunSuite
    with RegionsRDDUtil
    with ContigLengthsUtil {

  override def registrar = "org.hammerlab.guacamole.readsets.rdd.CoverageRDDSuiteRegistrar"

  lazy val readsRDD =
    makeRegionsRDD(
      numPartitions = 1,
      ("chr1", 100, 105,  1),
      ("chr1", 101, 106,  1),
      ("chr2",   8,   9,  1),
      ("chr2",   9,  11,  1),
      ("chr2", 102, 105,  1),
      ("chr2", 103, 106, 10),
      ("chr5",  90,  91, 10)
    )

  lazy val coverageRDD = new CoverageRDD(readsRDD)

  val contigLengths: ContigLengths =
    makeContigLengths(
      "chr1" -> 1000,
      "chr2" -> 1000,
      "chr5" -> 1000
    )

  test("all loci") {
    val loci = LociSet.all(contigLengths)

    val lociBroadcast = sc.broadcast(loci)

    val traversalCoverage = coverageRDD.traversalCoverage(halfWindowSize = 0, lociBroadcast)
    val explodedCoverage = coverageRDD.explodedCoverage(0, sc.broadcast(loci))

    val expected =
      List(
        "chr1:100" -> ( 1,  1),
        "chr1:101" -> ( 2,  1),
        "chr1:102" -> ( 2,  0),
        "chr1:103" -> ( 2,  0),
        "chr1:104" -> ( 2,  0),
        "chr1:105" -> ( 1,  0),
        "chr2:8"   -> ( 1,  1),
        "chr2:9"   -> ( 1,  1),
        "chr2:10"  -> ( 1,  0),
        "chr2:102" -> ( 1,  1),
        "chr2:103" -> (11, 10),
        "chr2:104" -> (11,  0),
        "chr2:105" -> (10,  0),
        "chr5:90"  -> (10, 10)
      )

    checkCoverage(traversalCoverage, expected)
    checkCoverage(explodedCoverage, expected)

    traversalCoverage.compare(explodedCoverage).stats should be(CmpStats(14))
  }

  test("some loci, half-window") {
    val loci = LociSet("chr1:102-107,chr2:7-20")

    val lociBroadcast = sc.broadcast(loci)

    val traversalCoverage = coverageRDD.traversalCoverage(halfWindowSize = 1, lociBroadcast)
    val explodedCoverage = coverageRDD.explodedCoverage(halfWindowSize = 1, lociBroadcast)

    val expected =
      List(
        "chr1:102" -> ( 2,  2),
        "chr1:103" -> ( 2,  0),
        "chr1:104" -> ( 2,  0),
        "chr1:105" -> ( 2,  0),
        "chr1:106" -> ( 1,  0),
        "chr2:7"   -> ( 1,  1),
        "chr2:8"   -> ( 2,  1),
        "chr2:9"   -> ( 2,  0),
        "chr2:10"  -> ( 1,  0),
        "chr2:11"  -> ( 1,  0)
      )

    checkCoverage(traversalCoverage, expected)
    checkCoverage(explodedCoverage, expected)

    traversalCoverage.compare(explodedCoverage).stats should be(CmpStats(10))
  }

  def checkCoverage(rdd: RDD[(Position, Coverage)], expected: List[(String, (Int, Int))]): Unit = {
    val actual = rdd.collect()
    val actualStrs =
      for {
        (pos, Coverage(depth, starts)) <- actual
      } yield {
        pos.toString -> (depth, starts)
      }

    actualStrs.toList should seqMatch[String, (Int, Int)](expected)
  }
}
