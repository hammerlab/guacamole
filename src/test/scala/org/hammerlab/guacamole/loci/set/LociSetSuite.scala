/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hammerlab.guacamole.loci.set

import org.hammerlab.guacamole.Common
import org.hammerlab.guacamole.loci.LociArgs
import org.hammerlab.guacamole.logging.DebugLogArgs
import org.hammerlab.guacamole.util.{GuacFunSuite, TestUtil}

class LociSetSuite extends GuacFunSuite {

  test("properties of empty LociSet") {
    val empty = LociSet()
    empty.contigs should have size (0)
    empty.count should equal(0)
    empty should equal(LociSet(""))
    val empty2 = LociSet("empty1:30-30,empty2:40-40")
    empty should equal(empty2)
  }

  test("count, containment, intersection testing of a loci set") {
    val set = LociSet("chr21:100-200,chr20:0-10,chr20:8-15,chr20:100-120,empty:10-10")
    set.contigs.map(_.name) should be(Seq("chr20", "chr21"))
    set.count should equal(135)
    set.onContig("chr20").contains(110) should be(true)
    set.onContig("chr20").contains(100) should be(true)
    set.onContig("chr20").contains(99) should be(false)
    set.onContig("chr20").contains(120) should be(false)
    set.onContig("chr20").contains(119) should be(true)
    set.onContig("chr20").count should be(35)
    set.onContig("chr20").intersects(0, 5) should be(true)
    set.onContig("chr20").intersects(0, 1) should be(true)
    set.onContig("chr20").intersects(0, 0) should be(false)
    set.onContig("chr20").intersects(7, 8) should be(true)
    set.onContig("chr20").intersects(9, 11) should be(true)
    set.onContig("chr20").intersects(11, 18) should be(true)
    set.onContig("chr20").intersects(18, 19) should be(false)
    set.onContig("chr20").intersects(14, 80) should be(true)
    set.onContig("chr20").intersects(15, 80) should be(false)
    set.onContig("chr20").intersects(120, 130) should be(false)
    set.onContig("chr20").intersects(119, 130) should be(true)

    set.onContig("chr21").contains(99) should be(false)
    set.onContig("chr21").contains(100) should be(true)
    set.onContig("chr21").contains(200) should be(false)
    set.onContig("chr21").count should be(100)
    set.onContig("chr21").intersects(110, 120) should be(true)
    set.onContig("chr21").intersects(90, 120) should be(true)
    set.onContig("chr21").intersects(150, 200) should be(true)
    set.onContig("chr21").intersects(150, 210) should be(true)
    set.onContig("chr21").intersects(200, 210) should be(false)
    set.onContig("chr21").intersects(201, 210) should be(false)
    set.onContig("chr21").intersects(90, 100) should be(false)
    set.onContig("chr21").intersects(90, 101) should be(true)
    set.onContig("chr21").intersects(90, 95) should be(false)
    set.onContig("chr21").iterator.toSeq should equal(100 until 200)
  }

  test("single loci parsing") {
    val set = LociSet("chr1:10000")
    set.count should be(1)
    set.onContig("chr1").contains( 9999) should be(false)
    set.onContig("chr1").contains(10000) should be(true)
    set.onContig("chr1").contains(10001) should be(false)
  }

  sparkTest("loci set invariants") {
    val sets = List(
      "",
      "empty:20-20,empty2:30-30",
      "20:100-200",
      "with_dots.and_underscores..2:100-200",
      "21:300-400",
      "X:5-17,X:19-22,Y:50-60",
      "chr21:100-200,chr20:0-10,chr20:8-15,chr20:100-120"
    ).map(LociSet(_))

    def checkInvariants(set: LociSet): Unit = {
      set should not be (null)
      set.toString should not be (null)
      withClue("invariants for: '%s'".format(set.toString)) {
        LociSet(set.toString) should equal(set)
        LociSet(set.toString).toString should equal(set.toString)
        set should equal(set)

        // Test serialization. We hit all sorts of null pointer exceptions here at one point, so we are paranoid about
        // checking every pointer.
        assert(sc != null)
        val parallelized = sc.parallelize(List(set))
        assert(parallelized != null)
        val collected = parallelized.collect()
        assert(collected != null)
        assert(collected.length == 1)
        val result = collected(0)
        result should equal(set)
      }
    }
    sets.foreach(checkInvariants)
  }

  sparkTest("loci argument parsing in Common") {
    class TestArgs extends DebugLogArgs with LociArgs {}

    // Test -loci argument
    val args1 = new TestArgs()
    args1.loci = "20:100-200"
    Common.lociFromArguments(args1).result should equal(LociSet("20:100-200"))

    // Test -loci-from-file argument. The test file gives a loci set equal to 20:100-200.
    val args2 = new TestArgs()
    args2.lociFromFile = TestUtil.testDataPath("loci.txt")
  }

  sparkTest("serialization: make an RDD[LociSet]") {
    val sets = List(
      "",
      "empty:20-20,empty2:30-30",
      "20:100-200",
      "21:300-400",
      "with_dots._and_..underscores11:900-1000",
      "X:5-17,X:19-22,Y:50-60",
      "chr21:100-200,chr20:0-10,chr20:8-15,chr20:100-120").map(LociSet(_))
    val rdd = sc.parallelize(sets)
    val result = rdd.map(_.toString).collect.toSeq
    result should equal(sets.map(_.toString))
  }

  sparkTest("serialization: make an RDD[LociSet], and an RDD[ContigLociSet]") {
    val sets = List(
      "",
      "empty:20-20,empty2:30-30",
      "20:100-200",
      "21:300-400",
      "X:5-17,X:19-22,Y:50-60",
      "chr21:100-200,chr20:0-10,chr20:8-15,chr20:100-120").map(LociSet(_))
    val rdd = sc.parallelize(sets)
    val result = rdd.map(set => {
      set.onContig("21").contains(5) // no op
      val ranges = set.onContig("21").ranges // no op
      set.onContig("20").toString
    }).collect.toSeq
    result should equal(sets.map(_.onContig("20").toString))
  }

  test("loci set parsing with contig lengths") {
    Builder(
      "chr1,chr2,17,chr2:3-5,chr20:10-20"
    )
    .result(
      "chr1" -> 10L,
      "chr2" -> 20L,
      "17" -> 12L,
      "chr20" -> 5000L
    )
    .toString should equal(
      "17:0-12,chr1:0-10,chr2:0-20,chr20:10-20"
    )
  }

  test("parse half-open interval") {
    Builder("chr1:10000-").result("chr1" -> 20000L).toString should be("chr1:10000-20000")
  }

  test("loci set single contig iterator basic") {
    val set = LociSet("chr1:20-25,chr1:15-17,chr1:40-43,chr1:40-42,chr1:5-5,chr2:5-6,chr2:6-7,chr2:2-4")
    set.onContig("chr1").iterator.toSeq should equal(Seq(15, 16, 20, 21, 22, 23, 24, 40, 41, 42))
    set.onContig("chr2").iterator.toSeq should equal(Seq(2, 3, 5, 6))

    val iter1 = set.onContig("chr1").iterator
    iter1.hasNext should be(true)
    iter1.head should be(15)
    iter1.next() should be(15)
    iter1.head should be(16)
    iter1.next() should be(16)
    iter1.head should be(20)
    iter1.next() should be(20)
    iter1.head should be(21)
    iter1.skipTo(23)
    iter1.next() should be(23)
    iter1.head should be(24)
    iter1.skipTo(5)
    iter1.head should be(24)
    iter1.skipTo(38)
    iter1.head should be(40)
    iter1.hasNext should be(true)
    iter1.skipTo(100)
    iter1.hasNext should be(false)
  }

  test("loci set single contig iterator: test that skipTo implemented efficiently.") {
    val set = LociSet("chr1:2-3,chr1:10-15,chr1:100-100000000000")

    val iter1 = set.onContig("chr1").iterator
    iter1.hasNext should be(true)
    iter1.head should be(2)
    iter1.next() should be(2)
    iter1.next() should be(10)
    iter1.next() should be(11)
    iter1.skipTo(6000000000L) // will hang if it steps through each locus.
    iter1.next() should be(6000000000L)
    iter1.next() should be(6000000001L)
    iter1.hasNext() should be(true)

    val iter2 = set.onContig("chr1").iterator
    iter2.skipTo(100000000000L)
    iter2.hasNext() should be(false)

    val iter3 = set.onContig("chr1").iterator
    iter3.skipTo(100000000000L - 1L)
    iter3.hasNext() should be(true)
    iter3.next() should be(100000000000L - 1L)
    iter3.hasNext() should be(false)
  }

  // We do not provide java serialization for LociSet, instead broadcasting it (which uses Kryo serialization).
  sparkTest("serialization: a closure that includes a LociSet") {
    val set = LociSet("chr21:100-200,chr20:0-10,chr20:8-15,chr20:100-120,empty:10-10")
    val setBC = sc.broadcast(set)
    val rdd = sc.parallelize(0L until 1000L)
    val result = rdd.filter(i => setBC.value.onContig("chr21").contains(i)).collect
    result should equal(100L until 200)
  }
}
