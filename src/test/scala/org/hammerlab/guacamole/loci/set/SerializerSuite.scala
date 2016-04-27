package org.hammerlab.guacamole.loci.set

import org.hammerlab.guacamole.util.GuacFunSuite

class SerializerSuite extends GuacFunSuite {
  sparkTest("make an RDD[LociSet]") {
    val sets =
      List(
        "",
        "empty:20-20,empty2:30-30",
        "20:100-200",
        "21:300-400",
        "with_dots._and_..underscores11:900-1000",
        "X:5-17,X:19-22,Y:50-60",
        "chr21:100-200,chr20:0-10,chr20:8-15,chr20:100-120"
      ).map(LociSet(_))

    val rdd = sc.parallelize(sets)
    val result = rdd.map(_.toString).collect.toSeq
    result should equal(sets.map(_.toString))
  }

  sparkTest("make an RDD[LociSet], and an RDD[Contig]") {
    val sets =
      List(
        "",
        "empty:20-20,empty2:30-30",
        "20:100-200",
        "21:300-400",
        "X:5-17,X:19-22,Y:50-60",
        "chr21:100-200,chr20:0-10,chr20:8-15,chr20:100-120"
      ).map(LociSet(_))

    val rdd = sc.parallelize(sets)

    val result = rdd.map(set => {
      set.onContig("21").contains(5)          // no op
      val ranges = set.onContig("21").ranges  // no op
      set.onContig("20").toString
    }).collect.toSeq

    result should equal(sets.map(_.onContig("20").toString))
  }
}
