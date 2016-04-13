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

package org.hammerlab.guacamole.loci.map

import org.hammerlab.guacamole.loci.set.LociSet
import org.hammerlab.guacamole.util.GuacFunSuite

class LociMapSuite extends GuacFunSuite {

  test("properties of empty LociMap") {
    val emptyMap = LociMap[String]()

    emptyMap.count should be(0)
    emptyMap.toString() should be("")
    emptyMap should equal(LociMap[String]())
  }

  test("basic map operations") {
    val lociMap = LociMap(
      ("chr1",  100L, 200L, "A"),
      ("chr20", 200L, 201L, "B")
    )

    lociMap.count should be(101)
    lociMap.toString should be("chr1:100-200=A,chr20:200-201=B")
    lociMap.contigs.map(_.name) should be(Seq("chr1", "chr20"))

    lociMap should equal(LociMap.union(LociMap("chr1", 100, 200, "A"), LociMap("chr20", 200, 201, "B")))
    lociMap should not equal (
      LociMap.union(LociMap("chr1", 100, 200, "A"), LociMap("chr20", 200, 201, "C")))
    lociMap should not equal (LociMap[String]())

    lociMap.asInverseMap should equal(
      Map(
        "A" -> LociSet("chr1:100-200"),
        "B" -> LociSet("chr20:200-201")
      )
    )

    lociMap.onContig("chr1").toString should equal("chr1:100-200=A")
    lociMap.onContig("chr20").toString should equal("chr20:200-201=B")

    // union clobbers the beginning of the first interval
    lociMap.union(LociMap("chr1", 100, 140, "X")).toString should be("chr1:100-140=X,chr1:140-200=A,chr20:200-201=B")
    lociMap.union(LociMap("chr1", 100, 140, "@")).toString should be("chr1:100-140=@,chr1:140-200=A,chr20:200-201=B")
  }

  test("asInverseMap with duplicate values") {
    val lociMap = LociMap(
      ("chr1", 100L, 200L, "A"),
      ("chr2", 200L, 300L, "A"),
      ("chr3", 400L, 500L, "B")
    )

    // asInverseMap stuffs all Loci with the same value into a LociSet.
    lociMap.asInverseMap should equal(
      Map(
        "A" -> LociSet("chr1:100-200,chr2:200-300"),
        "B" -> LociSet("chr3:400-500")
      )
    )
  }

  test("range coalescing") {
    val lociMap = LociMap(
      ("chr1", 100L, 200L, "A"),
      ("chr1", 400L, 500L, "B"),
      ("chr1", 150L, 160L, "C"),
      ("chr1", 180L, 240L, "A")
    )
    lociMap.toString should be("chr1:100-150=A,chr1:150-160=C,chr1:160-240=A,chr1:400-500=B")
  }

  test("union invariants") {
    val lociMap = LociMap.union(
      LociMap("chr1", 100, 200, "A"),
      LociMap("chr2", 200, 300, "A"),
      LociMap("chr3", 400, 500, "B"))

    lociMap.union(lociMap) should equal(lociMap)
    lociMap.union(LociMap[String]()) should equal(lociMap)  // union with empty map
  }
}
