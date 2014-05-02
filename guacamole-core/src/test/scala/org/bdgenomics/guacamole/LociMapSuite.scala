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

import org.scalatest.matchers.ShouldMatchers
import com.google.common.collect._
import org.scalatest.matchers.ShouldMatchers._

class LociMapSuite extends TestUtil.SparkFunSuite with ShouldMatchers {

  test("properties of empty LociMap") {
    val emptyMap = LociMap[String]()

    emptyMap.count should be(0)
    emptyMap.toString() should be("")
    emptyMap should equal(LociMap.newBuilder[String].result)
  }

  test("basic map operations") {
    val lociMap = LociMap.newBuilder[String]
      .put("chr1", 100, 200, "A")
      .put("chr20", 200, 201, "B")
      .result

    lociMap.count should be(101)
    lociMap.toString should be("chr1:100-200=A,chr20:200-201=B")
    lociMap.contigs should be(List("chr1", "chr20"))

    lociMap should equal(LociMap.union(LociMap("chr1", 100, 200, "A"), LociMap("chr20", 200, 201, "B")))
    lociMap should not equal (
      LociMap.union(LociMap("chr1", 100, 200, "A"), LociMap("chr20", 200, 201, "C")))
    lociMap should not equal (LociMap[String]())

    lociMap.asInverseMap should equal(
      List("A" -> LociSet.parse("chr1:100-200"), "B" -> LociSet.parse("chr20:200-201")).toMap)

    lociMap.onContig("chr1").toString should equal("chr1:100-200=A")
    lociMap.onContig("chr20").toString should equal("chr20:200-201=B")

    // union clobbers the beginning of the first interval
    lociMap.union(LociMap("chr1", 100, 140, "X")).toString should be("chr1:100-140=X,chr1:140-200=A,chr20:200-201=B")
  }

  test("asInverseMap with duplicate values") {
    val lociMap = LociMap.union(
      LociMap("chr1", 100, 200, "A"),
      LociMap("chr2", 200, 300, "A"),
      LociMap("chr3", 400, 500, "B"))

    // asInverseMap stuffs all Loci with the same value into a LociSet.
    lociMap.asInverseMap should equal(
      List("A" -> LociSet.parse("chr1:100-200,chr2:200-300"), "B" -> LociSet.parse("chr3:400-500")).toMap)
  }

  test("union invariants") {
    val lociMap = LociMap.union(
      LociMap("chr1", 100, 200, "A"),
      LociMap("chr2", 200, 300, "A"),
      LociMap("chr3", 400, 500, "B"))

    lociMap.union(lociMap) should equal(lociMap)
    lociMap.union(LociMap[String]()) should equal(lociMap) // union with empty map
  }

  test("SingleContig, empty") {
    type JLong = java.lang.Long
    val contigMap = LociMap.SingleContig("chr1", ImmutableRangeMap.builder[JLong, String]().build())

    contigMap.get(100) should be(None)
    contigMap.getAll(0, 10000) should equal(Set())
    contigMap.contains(100) should be(false)
    contigMap.count should be(0)
    contigMap.ranges should equal(Set())
    contigMap.numRanges should be(0)
    contigMap.isEmpty should be(true)
    contigMap.toString should be("")
  }

  test("SingleContig, basic operations") {
    type JLong = java.lang.Long
    val range100to200 = ImmutableRangeMap.of[JLong, String](Range.closedOpen[JLong](100, 200), "A")
    val range200to300 = ImmutableRangeMap.of[JLong, String](Range.closedOpen[JLong](200, 300), "B")

    val contigMap = LociMap.SingleContig("chr1",
      ImmutableRangeMap.builder[JLong, String]().putAll(range100to200).putAll(range200to300).build())

    contigMap.get(99) should be(None)
    contigMap.get(100) should be(Some("A"))
    contigMap.get(199) should be(Some("A"))
    contigMap.get(200) should be(Some("B"))
    contigMap.get(299) should be(Some("B"))
    contigMap.get(300) should be(None)

    contigMap.getAll(0, 100) should equal(Set())
    contigMap.getAll(0, 101) should equal(Set("A"))
    contigMap.getAll(199, 200) should equal(Set("A"))
    contigMap.getAll(199, 201) should equal(Set("A", "B"))
    contigMap.getAll(200, 201) should equal(Set("B"))
    contigMap.getAll(0, 10000) should equal(Set("A", "B"))

    contigMap.contains(0) should be(false)
    contigMap.contains(99) should be(false)
    contigMap.contains(100) should be(true)
    contigMap.contains(200) should be(true)
    contigMap.contains(299) should be(true)
    contigMap.contains(300) should be(false)

    contigMap.count should be(200)
    contigMap.isEmpty should be(false)
    contigMap.toString should be("chr1:100-200=A,chr1:200-300=B")

    val emptyMap = LociMap.SingleContig("chr1", ImmutableRangeMap.builder[JLong, String]().build())
    contigMap.union(contigMap) should equal(contigMap)
    contigMap.union(emptyMap) should equal(contigMap)

    val emptyMapWrongContig = LociMap.SingleContig("chr2", ImmutableRangeMap.builder[JLong, String]().build())
    val caught = evaluating { contigMap.union(emptyMapWrongContig) } should produce[AssertionError]
    caught.getMessage should include("different contigs: chr1 and chr2")
  }
}