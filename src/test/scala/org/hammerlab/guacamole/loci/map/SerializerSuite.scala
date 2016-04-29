package org.hammerlab.guacamole.loci.map

import org.apache.spark.SparkEnv
import org.hammerlab.guacamole.util.GuacFunSuite

class SerializerSuite extends GuacFunSuite {
  def testSerde(
    name: String
  )(
    ranges: (String, Long, Long, String)*
  )(
    expectedBytes: Int,
    numRanges: Int,
    count: Int
  ) = {
    test(name) {
      val serializer = SparkEnv.get.serializer.newInstance()

      val beforeMap = LociMap[String](ranges: _*)

      beforeMap.onContig("chr1").asMap.size should be(numRanges)
      beforeMap.onContig("chr1").count should be(count)

      val bytes = serializer.serialize(beforeMap)
      bytes.array.length should be(expectedBytes)

      val afterMap: LociMap[String] = serializer.deserialize[LociMap[String]](bytes)

      beforeMap should be(afterMap)
    }
  }

  testSerde("empty")()(10, 0, 0)

  testSerde("1")(
    ("chr1", 100L, 200L, "A")
  )(43, 1, 100)

  testSerde("2")(
    ("chr1", 100L, 200L, "A"),
    ("chr1", 400L, 500L, "B")
  )(63, 2, 200)

  testSerde("3")(
    ("chr1", 100L, 200L, "A"),
    ("chr1", 400L, 500L, "B"),
    ("chr1", 600L, 700L, "C")
  )(83, 3, 300)

  testSerde("4")(
    ("chr1", 100L, 200L, "A"),
    ("chr1", 400L, 500L, "B"),
    ("chr1", 600L, 700L, "C"),
    ("chr1", 700L, 800L, "A")
  )(101, 4, 400)
}
