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

    lociMap should equal(LociMap.union(LociMap("chr1", 100, 200, "A"), LociMap("chr20", 200, 201, "B")))
    lociMap should not equal(
      LociMap.union(LociMap("chr1", 100, 200, "A"), LociMap("chr20", 200, 201, "C")))
    lociMap should not equal(LociMap[String]())

    lociMap.asInverseMap should equal(
      List("A" -> LociSet.parse("chr1:100-200"), "B" -> LociSet.parse("chr20:200-201")).toMap)

    lociMap.onContig("chr1").toString should equal("chr1:100-200=A")
    lociMap.onContig("chr20").toString should equal("chr20:200-201=B")

    // union clobbers the beginning of the first interval
    lociMap.union(LociMap("chr1", 100, 140, "X")).toString should be("chr1:100-140=X,chr1:140-200=A,chr20:200-201=B")
  }

  // test("asInverseMap with duplicate values")
  // test("SingleContig")
}