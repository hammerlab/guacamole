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

package org.hammerlab.guacamole.variants

import org.apache.spark.SparkEnv
import org.hammerlab.guacamole.{ Bases, TestUtil }
import org.hammerlab.guacamole.TestUtil.SparkFunSuite
import org.scalatest.FunSuite

class CalledAlleleSuite extends FunSuite with SparkFunSuite {

  test("serializing called genotype") {
    val gt = CalledAllele("sample",
      "chr1",
      123456789123L,
      Allele(Seq(Bases.T), Seq(Bases.A)),
      evidence = AlleleEvidence(0.99, 15, 10, 10, 5, 60, 30))

    val serialized = TestUtil.serialize(gt)
    val deserialized = TestUtil.deserialize[CalledAllele](serialized)

    assert(gt === deserialized)
  }

  test("serializing called somatic genotype") {

    val sgt = new CalledSomaticAllele("sample",
      "chr1",
      123456789123L,
      Allele(Seq(Bases.T), Seq(Bases.A)),
      0.99 / 0.01,
      tumorEvidence = AlleleEvidence(0.99, 15, 10, 10, 5, 60, 30),
      normalEvidence = AlleleEvidence(0.01, 17, 0, 10, 0, 60, 30))

    val serialized = TestUtil.serialize(sgt)
    val deserialized = TestUtil.deserialize[CalledSomaticAllele](serialized)

    assert(sgt === deserialized)

  }

  sparkTest("serializing multi-base called somatic genotype") {

    val serializer = SparkEnv.get.serializer.newInstance()

    val sgt = new CalledSomaticAllele("sample",
      "chr1",
      123456789123L,
      Allele(Seq(Bases.T), Seq(Bases.T, Bases.A, Bases.T)),
      0.99 / 0.01,
      tumorEvidence = AlleleEvidence(0.99, 15, 10, 10, 5, 60, 30),
      normalEvidence = AlleleEvidence(0.01, 17, 0, 10, 0, 60, 30))

    val serialized = serializer.serialize(sgt)
    val deserialized = serializer.deserialize[CalledSomaticAllele](serialized)

    assert(sgt === deserialized)

  }

}
