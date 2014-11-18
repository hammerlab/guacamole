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

package org.bdgenomics.guacamole

import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.FileUtil
import org.bdgenomics.guacamole.reads.Read
import org.scalatest.Matchers

class LociSetSuite extends TestUtil.SparkFunSuite with Matchers {

  test("properties of empty LociSet") {
    LociSet.empty.contigs should have length (0)
    LociSet.empty.count should equal(0)
    LociSet.empty should equal(LociSet.parse(""))
    LociSet.empty should equal(LociSet.parse("empty1:30-30,empty2:40-40"))
  }

  test("count, containment, intersection testing of a loci set") {
    val set = LociSet.parse("chr21:100-200,chr20:0-10,chr20:8-15,chr20:100-120,empty:10-10")
    set.contigs should equal(List("chr20", "chr21"))
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
    set.onContig("chr21").individually.toSeq should equal(100 until 200)
  }

  sparkTest("loci set invariants") {
    val sets = List(
      "",
      "empty:20-20,empty2:30-30",
      "20:100-200",
      "with_dots.and_underscores..2:100-200",
      "21:300-400",
      "X:5-17,X:19-22,Y:50-60",
      "chr21:100-200,chr20:0-10,chr20:8-15,chr20:100-120").map(LociSet.parse)

    def check_invariants(set: LociSet): Unit = {
      set should not be (null)
      set.toString should not be (null)
      withClue("invariants for: '%s'".format(set.toString)) {
        LociSet.parse(set.toString) should equal(set)
        LociSet.parse(set.toString).toString should equal(set.toString)
        set should equal(set)
        set should not equal (set.union(LociSet.parse("abc123:30-40")))
        set should equal(set.union(LociSet.parse("empty:99-99")))

        // Test serialization. We hit all sorts of null pointer exceptions here at once point, so we are paranoid about
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
    sets.foreach(check_invariants)
    check_invariants(LociSet.union(sets: _*))
  }

  sparkTest("loci argument parsing in Common") {
    val read = TestUtil.makeRead("C", "1M", "1", 500, "20")
    val emptyReadSet = ReadSet(sc.parallelize(Seq(read)), None, "", Read.InputFilters.empty, 0, false)
    class TestArgs extends Common.Arguments.Base with Common.Arguments.Loci {}

    // Test -loci argument
    val args1 = new TestArgs()
    args1.loci = "20:100-200"
    Common.loci(args1, emptyReadSet) should equal(LociSet.parse("20:100-200"))

    // Test -loci-from-file argument. The test file gives a loci set equal to 20:100-200.
    val args2 = new TestArgs()
    args2.lociFromFile = TestUtil.testDataPath("loci.txt")
    Common.loci(args2, emptyReadSet) should equal(LociSet.parse("20:100-200"))
  }

  sparkTest("serialization: make an RDD[LociSet]") {
    val sets = List(
      "",
      "empty:20-20,empty2:30-30",
      "20:100-200",
      "21:300-400",
      "with_dots._and_..underscores11:900-1000",
      "X:5-17,X:19-22,Y:50-60",
      "chr21:100-200,chr20:0-10,chr20:8-15,chr20:100-120").map(LociSet.parse)
    val rdd = sc.parallelize(sets)
    val result = rdd.map(_.toString).collect.toSeq
    result should equal(sets.map(_.toString).toSeq)
  }

  sparkTest("serialization: make an RDD[LociSet], and an RDD[LociSet.SingleContig]") {
    val sets = List(
      "",
      "empty:20-20,empty2:30-30",
      "20:100-200",
      "21:300-400",
      "X:5-17,X:19-22,Y:50-60",
      "chr21:100-200,chr20:0-10,chr20:8-15,chr20:100-120").map(LociSet.parse)
    val rdd = sc.parallelize(sets)
    val result = rdd.map(set => {
      set.onContig("21").contains(5) // no op
      val ranges = set.onContig("21").ranges // no op
      set.onContig("20").toString
    }).collect.toSeq
    result should equal(sets.map(_.onContig("20").toString).toSeq)
  }

  // This test currently fails, because we do not provide java serialization for LociSet. Instead of 
  // including LociSet instances in closures, we are currently getting around it by broadcasting
  // these objects, which will use Kryo serialization (which we implement for LociSet). This approach
  // may actually be more efficient anyway.
  /*
  sparkTest("serialization: a closure that includes a LociSet") {
    val set = LociSet.parse("chr21:100-200,chr20:0-10,chr20:8-15,chr20:100-120,empty:10-10")
    val rdd = sc.parallelize(0L until 1000L)
    val result = rdd.filter(i => set.onContig("chr21").contains(i)).collect
    result should equal(100L until 200)
  }
  */
}
